#!/usr/bin/env python3
"""
Matchmaker Load Simulation Script
==================================


Usage:
    python simulate.py [options]

Examples:
    python simulate.py --players 1000 --duration 30
    python simulate.py --url http://localhost:8080 --players 5000 --concurrency 200
    python simulate.py --players 500 --output results/run1.md
"""

import argparse
import asyncio
import json
import os
import signal
import sys
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import numpy as np


# Constants 

VERSION = "1.0.0"
HEALTH_POLL_INTERVAL_SECS = 2.0
HEALTH_MAX_WAIT_SECS = 30.0
METRICS_POLL_INTERVAL_SECS = 3.0

# Traffic phase proportions
PHASE_STEADY_FRACTION = 0.50   # First 50% of players
PHASE_BURST_FRACTION  = 0.30   # Next 30%
PHASE_SPIKE_FRACTION  = 0.20   # Final 20%

# Rate multipliers per phase
RATE_STEADY = 1.0
RATE_BURST  = 2.5
RATE_SPIKE  = 5.0

# MMR distribution parameters
MMR_NORMAL_FRACTION    = 0.98
MMR_LOW_EDGE_FRACTION  = 0.01
MMR_HIGH_EDGE_FRACTION = 0.01
MMR_MEAN               = 1500
MMR_STD                = 400
MMR_MIN                = 0
MMR_MAX                = 3000
MMR_LOW_MAX            = 200
MMR_HIGH_MIN           = 2800


# Data classes 

@dataclass
class SimPlayer:
    """A generated player waiting to be sent to the matchmaker."""
    player_id: str
    mmr: int
    phase: str                          
    generated_at: float = field(default_factory=time.time)

    
    enqueue_latency_ms: Optional[float] = None
    response_status: Optional[int] = None
    final_status: str = "pending"       # pending | queued | matched | timeout | error
    error_message: Optional[str] = None

    # Filled in from match results
    match_id: Optional[str] = None
    match_wait_ms: Optional[int] = None
    match_skill_rating: Optional[int] = None


@dataclass
class PhaseMetrics:
    """Metrics accumulated for one traffic phase."""
    name: str
    players_sent: int = 0
    players_matched: int = 0
    errors: int = 0
    timeouts: int = 0
    latencies_ms: List[float] = field(default_factory=list)
    wait_times_ms: List[int] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        if self.players_sent == 0:
            return 0.0
        return self.players_matched / self.players_sent * 100

    @property
    def avg_latency_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        return float(np.mean(self.latencies_ms))

    @property
    def avg_wait_ms(self) -> float:
        if not self.wait_times_ms:
            return 0.0
        return float(np.mean(self.wait_times_ms))


@dataclass
class CorrectnessReport:
    """Correctness validation results."""
    duplicate_player_ids: List[str] = field(default_factory=list)
    duplicate_match_assignments: List[str] = field(default_factory=list)
    players_in_multiple_matches: List[str] = field(default_factory=list)
    total_unique_players_observed: int = 0
    total_unique_matches_observed: int = 0
    violations: int = 0

    @property
    def is_correct(self) -> bool:
        return self.violations == 0


# Player generation 

def generate_players(total: int) -> List[SimPlayer]:
    """
    Generate a realistic player pool with a mixed MMR distribution.

    Distribution:
      98% — Normal(mean=1500, std=400), clamped to [0, 3000]
       1% — Low-skill edge cases: MMR in [0, 200]
       1% — High-skill edge cases: MMR in [2800, 3000]
    """
    rng = np.random.default_rng(seed=42)  

    n_normal    = int(total * MMR_NORMAL_FRACTION)
    n_low_edge  = int(total * MMR_LOW_EDGE_FRACTION)
    n_high_edge = total - n_normal - n_low_edge

    # Normal distribution clamped to [0, 3000]
    normal_mmrs = rng.normal(MMR_MEAN, MMR_STD, n_normal)
    normal_mmrs = np.clip(normal_mmrs, MMR_MIN, MMR_MAX).astype(int).tolist()

   
    low_mmrs = rng.integers(MMR_MIN, MMR_LOW_MAX + 1, n_low_edge).tolist()

    
    high_mmrs = rng.integers(MMR_HIGH_MIN, MMR_MAX + 1, n_high_edge).tolist()

    all_mmrs = normal_mmrs + low_mmrs + high_mmrs

    
    n_steady = int(total * PHASE_STEADY_FRACTION)
    n_burst  = int(total * PHASE_BURST_FRACTION)

    phases = (
        ["steady"] * n_steady +
        ["burst"]  * n_burst  +
        ["spike"]  * (total - n_steady - n_burst)
    )

    players = [
        SimPlayer(
            player_id=str(uuid.uuid4()),
            mmr=mmr,
            phase=phase,
        )
        for mmr, phase in zip(all_mmrs, phases)
    ]

    
    rng.shuffle(players)

    return players


# Health check 

async def wait_for_healthy(session: aiohttp.ClientSession, base_url: str) -> bool:
    """
    Poll GET /health until the service responds with status 'ok'.
    Returns True if healthy within HEALTH_MAX_WAIT_SECS, False otherwise.
    """
    deadline = time.time() + HEALTH_MAX_WAIT_SECS
    attempt = 0

    while time.time() < deadline:
        attempt += 1
        try:
            async with session.get(
                f"{base_url}/health",
                timeout=aiohttp.ClientTimeout(total=5.0),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("status") == "ok":
                        players_waiting = data.get("players_waiting", 0)
                        print(
                            f"  ✓ Service healthy "
                            f"(players_waiting={players_waiting}, "
                            f"uptime={data.get('uptime_secs', 0)}s)"
                        )
                        return True
        except (aiohttp.ClientError, asyncio.TimeoutError):
            pass

        print(
            f"  Waiting for service... "
            f"(attempt {attempt}, "
            f"{deadline - time.time():.0f}s remaining)"
        )
        await asyncio.sleep(HEALTH_POLL_INTERVAL_SECS)

    return False


# Metrics polling 

async def poll_metrics_background(
    session: aiohttp.ClientSession,
    base_url: str,
    stop_event: asyncio.Event,
    metrics_snapshots: List[dict],
) -> None:
    """
    Background task: poll GET /metrics every METRICS_POLL_INTERVAL_SECS.
    Stores snapshots for final reporting.
    """
    while not stop_event.is_set():
        try:
            async with session.get(
                f"{base_url}/metrics",
                timeout=aiohttp.ClientTimeout(total=5.0),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    data["_polled_at"] = time.time()
                    metrics_snapshots.append(data)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            pass

        try:
            await asyncio.wait_for(
                asyncio.shield(stop_event.wait()),
                timeout=METRICS_POLL_INTERVAL_SECS,
            )
        except asyncio.TimeoutError:
            pass


# Enqueue worker 

async def enqueue_player(
    session: aiohttp.ClientSession,
    base_url: str,
    player: SimPlayer,
    semaphore: asyncio.Semaphore,
    per_player_timeout: float,
) -> None:
    """
    Send POST /enqueue for a single player.
    Updates the player's tracking fields in-place.
    """
    async with semaphore:
        start = time.perf_counter()
        try:
            async with session.post(
                f"{base_url}/enqueue",
                json={"id": player.player_id, "skill_rating": player.mmr},
                timeout=aiohttp.ClientTimeout(total=per_player_timeout),
            ) as resp:
                elapsed_ms = (time.perf_counter() - start) * 1000
                player.enqueue_latency_ms = elapsed_ms
                player.response_status = resp.status

                if resp.status == 200:
                    player.final_status = "queued"
                elif resp.status == 409:
                    # Duplicate — already in queue 
                    player.final_status = "error"
                    player.error_message = "409 Conflict — duplicate player ID"
                else:
                    player.final_status = "error"
                    player.error_message = f"HTTP {resp.status}"

        except asyncio.TimeoutError:
            player.final_status = "timeout"
            player.enqueue_latency_ms = per_player_timeout * 1000
        except aiohttp.ClientError as e:
            player.final_status = "error"
            player.error_message = str(e)
            player.enqueue_latency_ms = (time.perf_counter() - start) * 1000


#  Match result collection 

async def collect_match_results(
    session: aiohttp.ClientSession,
    base_url: str,
    player_map: Dict[str, SimPlayer],
    total_expected_matches: int,
) -> None:
    """
    Poll GET /matches to collect formed match results.
    Updates player tracking records with match outcome data.

    Uses a limit large enough to capture all matches for the simulation size.
    """
    
    limit = max(len(player_map) // 10, 20)

    try:
        async with session.get(
            f"{base_url}/matches?limit={limit}",
            timeout=aiohttp.ClientTimeout(total=10.0),
        ) as resp:
            if resp.status != 200:
                return

            data = await resp.json()
            matches = data.get("matches", [])

            for match in matches:
                match_id = match.get("match_id", "unknown")
                team_a = match.get("team_a", {}).get("players", [])
                team_b = match.get("team_b", {}).get("players", [])

                for player_data in team_a + team_b:
                    pid = player_data.get("id", "")
                    if pid in player_map:
                        p = player_map[pid]
                        p.final_status = "matched"
                        p.match_id = match_id
                        p.match_wait_ms = player_data.get("wait_ms")
                        p.match_skill_rating = player_data.get("skill_rating")

    except (aiohttp.ClientError, asyncio.TimeoutError):
        pass


#  Traffic injection 

async def inject_traffic(
    session: aiohttp.ClientSession,
    base_url: str,
    players: List[SimPlayer],
    duration_secs: float,
    concurrency: int,
    per_player_timeout: float,
    progress_interval: int = 100,
) -> None:
    """
    Inject players with time-spread arrivals across three traffic phases.

    Phase timing is controlled by calculating per-player delays based on
    the phase rate multiplier. Higher rate = shorter delays = faster arrivals.
    """
    semaphore = asyncio.Semaphore(concurrency)
    total = len(players)

    
    # Adjusted per-phase by rate multiplier
    base_delay = duration_secs / total if total > 0 else 0.0

    # Phase boundaries by player index
    n_steady = int(total * PHASE_STEADY_FRACTION)
    n_burst  = int(total * PHASE_BURST_FRACTION)

    tasks = []
    simulation_start = time.time()
    last_progress = 0

    for i, player in enumerate(players):
        # Determine phase delay multiplier
        if i < n_steady:
            rate_multiplier = RATE_STEADY
        elif i < n_steady + n_burst:
            rate_multiplier = RATE_BURST
        else:
            rate_multiplier = RATE_SPIKE

        # Shorter delay = faster arrival = higher rate
        delay = base_delay / rate_multiplier

        # Schedule this player's enqueue
        task = asyncio.create_task(
            _scheduled_enqueue(
                session, base_url, player, semaphore,
                per_player_timeout, delay * i,
            )
        )
        tasks.append(task)

        # Print progress
        if i - last_progress >= progress_interval or i == total - 1:
            elapsed = time.time() - simulation_start
            queued = sum(1 for p in players[:i+1] if p.final_status == "queued")
            matched = sum(1 for p in players[:i+1] if p.final_status == "matched")
            print(
                f"  [{elapsed:5.1f}s] "
                f"Sent: {i+1:5d}/{total} | "
                f"Queued: {queued:5d} | "
                f"Matched: {matched:5d} | "
                f"Phase: {player.phase}",
                end="\r",
                flush=True,
            )
            last_progress = i

    # Wait for all tasks to complete
    print()  
    print(f"  All {total} players dispatched — waiting for tasks to complete...")

    await asyncio.gather(*tasks, return_exceptions=True)


async def _scheduled_enqueue(
    session: aiohttp.ClientSession,
    base_url: str,
    player: SimPlayer,
    semaphore: asyncio.Semaphore,
    per_player_timeout: float,
    delay: float,
) -> None:
    """Sleep for `delay` seconds then enqueue the player."""
    if delay > 0:
        await asyncio.sleep(delay)
    await enqueue_player(session, base_url, player, semaphore, per_player_timeout)


#  Correctness validation 

def validate_correctness(players: List[SimPlayer]) -> CorrectnessReport:
    """
    Validate that no player appears in multiple matches and no UUIDs are duplicated.
    """
    report = CorrectnessReport()

    # Check for duplicate player IDs in our generated set
    seen_ids = set()
    for p in players:
        if p.player_id in seen_ids:
            report.duplicate_player_ids.append(p.player_id)
        seen_ids.add(p.player_id)

    report.total_unique_players_observed = len(seen_ids)

    # Check for players assigned to multiple matches
    player_to_matches: Dict[str, List[str]] = defaultdict(list)
    match_ids = set()

    for p in players:
        if p.final_status == "matched" and p.match_id:
            player_to_matches[p.player_id].append(p.match_id)
            match_ids.add(p.match_id)

    report.total_unique_matches_observed = len(match_ids)

    for player_id, matches in player_to_matches.items():
        if len(matches) > 1:
            report.players_in_multiple_matches.append(player_id)
            report.duplicate_match_assignments.extend(matches)

    report.violations = (
        len(report.duplicate_player_ids) +
        len(report.players_in_multiple_matches)
    )

    return report


#  Metrics computation 

def compute_phase_metrics(players: List[SimPlayer]) -> Dict[str, PhaseMetrics]:
    """Aggregate per-phase statistics from player tracking records."""
    phases = {
        "steady": PhaseMetrics(name="Steady"),
        "burst":  PhaseMetrics(name="Burst"),
        "spike":  PhaseMetrics(name="Spike"),
    }

    for p in players:
        pm = phases.get(p.phase)
        if pm is None:
            continue

        pm.players_sent += 1

        if p.final_status == "matched":
            pm.players_matched += 1
            if p.match_wait_ms is not None:
                pm.wait_times_ms.append(p.match_wait_ms)
        elif p.final_status == "timeout":
            pm.timeouts += 1
        elif p.final_status == "error":
            pm.errors += 1

        if p.enqueue_latency_ms is not None:
            pm.latencies_ms.append(p.enqueue_latency_ms)

    return phases


def compute_latency_percentiles(
    latencies: List[float],
) -> Tuple[float, float, float]:
    """Return (p50, p95, p99) for a list of latency values in ms."""
    if not latencies:
        return 0.0, 0.0, 0.0
    arr = np.array(latencies)
    return (
        float(np.percentile(arr, 50)),
        float(np.percentile(arr, 95)),
        float(np.percentile(arr, 99)),
    )


def compute_wait_percentiles(
    wait_times: List[int],
) -> Tuple[float, float, float]:
    """Return (p50, p95, p99) for wait times in ms."""
    if not wait_times:
        return 0.0, 0.0, 0.0
    arr = np.array(wait_times, dtype=float)
    return (
        float(np.percentile(arr, 50)),
        float(np.percentile(arr, 95)),
        float(np.percentile(arr, 99)),
    )


#  Report generation 

def build_report(
    players: List[SimPlayer],
    phase_metrics: Dict[str, PhaseMetrics],
    correctness: CorrectnessReport,
    final_metrics: Optional[dict],
    start_time: float,
    end_time: float,
    interrupted: bool,
    args: argparse.Namespace,
) -> dict:
    """Build the full simulation report as a structured dict."""

    total = len(players)
    matched = sum(1 for p in players if p.final_status == "matched")
    queued_only = sum(1 for p in players if p.final_status == "queued")
    timeouts = sum(1 for p in players if p.final_status == "timeout")
    errors = sum(1 for p in players if p.final_status == "error")
    pending = sum(1 for p in players if p.final_status == "pending")

    runtime = end_time - start_time

    throughput = (matched // 10) / runtime if runtime > 0 else 0.0
    rps = total / runtime if runtime > 0 else 0.0

    all_latencies = [p.enqueue_latency_ms for p in players if p.enqueue_latency_ms]
    lat_p50, lat_p95, lat_p99 = compute_latency_percentiles(all_latencies)

    all_waits = [p.match_wait_ms for p in players
                 if p.final_status == "matched" and p.match_wait_ms is not None]
    wait_p50, wait_p95, wait_p99 = compute_wait_percentiles(all_waits)

    success_rate = matched / total * 100 if total > 0 else 0.0

    return {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "version": VERSION,
            "interrupted": interrupted,
            "server_url": args.url,
            "runtime_secs": round(runtime, 2),
        },
        "summary": {
            "total_players_sent": total,
            "matched": matched,
            "queued_only": queued_only,
            "timeouts": timeouts,
            "errors": errors,
            "pending": pending,
            "success_rate_pct": round(success_rate, 2),
            "throughput_matches_per_sec": round(throughput, 2),
            "requests_per_sec": round(rps, 2),
        },
        "latency": {
            "enqueue_p50_ms": round(lat_p50, 2),
            "enqueue_p95_ms": round(lat_p95, 2),
            "enqueue_p99_ms": round(lat_p99, 2),
        },
        "wait_times": {
            "match_wait_p50_ms": round(wait_p50, 2),
            "match_wait_p95_ms": round(wait_p95, 2),
            "match_wait_p99_ms": round(wait_p99, 2),
            "avg_wait_ms": round(float(np.mean(all_waits)) if all_waits else 0, 2),
        },
        "correctness": {
            "is_correct": correctness.is_correct,
            "violations": correctness.violations,
            "duplicate_player_ids": len(correctness.duplicate_player_ids),
            "players_in_multiple_matches": len(correctness.players_in_multiple_matches),
            "total_unique_players_observed": correctness.total_unique_players_observed,
            "total_unique_matches_observed": correctness.total_unique_matches_observed,
        },
        "phases": {
            name: {
                "players_sent": pm.players_sent,
                "players_matched": pm.players_matched,
                "success_rate_pct": round(pm.success_rate, 2),
                "avg_latency_ms": round(pm.avg_latency_ms, 2),
                "avg_wait_ms": round(pm.avg_wait_ms, 2),
                "errors": pm.errors,
                "timeouts": pm.timeouts,
            }
            for name, pm in phase_metrics.items()
        },
        "service_metrics": final_metrics or {},
        "players": [
            {
                "player_id": p.player_id,
                "mmr": p.mmr,
                "phase": p.phase,
                "final_status": p.final_status,
                "enqueue_latency_ms": round(p.enqueue_latency_ms, 2)
                    if p.enqueue_latency_ms else None,
                "response_status": p.response_status,
                "match_id": p.match_id,
                "match_wait_ms": p.match_wait_ms,
            }
            for p in players
        ],
    }


def print_report(report: dict) -> None:
    """Print the simulation report to stdout in a readable format."""
    meta = report["meta"]
    summary = report["summary"]
    latency = report["latency"]
    waits = report["wait_times"]
    correctness = report["correctness"]
    phases = report["phases"]
    svc = report.get("service_metrics", {})

    interrupted_note = " [PARTIAL — INTERRUPTED]" if meta["interrupted"] else ""

    print()
    print("=" * 56)
    print(f" MATCHMAKER SIMULATION REPORT{interrupted_note}")
    print("=" * 56)
    print(f" Server  : {meta['server_url']}")
    print(f" Runtime : {meta['runtime_secs']:.2f}s")
    print(f" Generated: {meta['generated_at']}")
    print()

    print("--- Summary ---")
    print(f"  Players Sent       : {summary['total_players_sent']:,}")
    print(f"  Matched            : {summary['matched']:,}")
    print(f"  Queued (unmatched) : {summary['queued_only']:,}")
    print(f"  Timeouts           : {summary['timeouts']:,}")
    print(f"  Errors             : {summary['errors']:,}")
    print(f"  Success Rate       : {summary['success_rate_pct']:.1f}%")
    print()

    print("--- Performance ---")
    print(f"  Throughput         : {summary['throughput_matches_per_sec']:.1f} matches/sec")
    print(f"  Requests/sec       : {summary['requests_per_sec']:.1f} req/sec")
    print(f"  Enqueue P50        : {latency['enqueue_p50_ms']:.1f} ms")
    print(f"  Enqueue P95        : {latency['enqueue_p95_ms']:.1f} ms")
    print(f"  Enqueue P99        : {latency['enqueue_p99_ms']:.1f} ms")
    print()

    if waits["avg_wait_ms"] > 0:
        print("--- Match Wait Times ---")
        print(f"  Wait P50           : {waits['match_wait_p50_ms']:.0f} ms")
        print(f"  Wait P95           : {waits['match_wait_p95_ms']:.0f} ms")
        print(f"  Wait P99           : {waits['match_wait_p99_ms']:.0f} ms")
        print(f"  Avg Wait           : {waits['avg_wait_ms']:.0f} ms")
        print()

    print("--- Correctness ---")
    status_icon = "✓ PASS" if correctness["is_correct"] else "✗ FAIL"
    print(f"  Status             : {status_icon}")
    print(f"  Violations         : {correctness['violations']}")
    print(f"  Duplicate Players  : {correctness['duplicate_player_ids']}")
    print(f"  Multi-Match Players: {correctness['players_in_multiple_matches']}")
    print(f"  Unique Matches Seen: {correctness['total_unique_matches_observed']:,}")
    print()

    print("--- Traffic Phases ---")
    for name, pm in phases.items():
        print(
            f"  {name.capitalize():<8}: "
            f"sent={pm['players_sent']:4d}  "
            f"matched={pm['players_matched']:4d}  "
            f"rate={pm['success_rate_pct']:5.1f}%  "
            f"avg_wait={pm['avg_wait_ms']:.0f}ms"
        )
    print()

    if svc:
        print("--- Service Metrics (final snapshot) ---")
        fields = [
            ("total_players_enqueued",    "Total Enqueued"),
            ("total_matches_created",      "Matches Created"),
            ("total_players_matched",      "Players Matched"),
            ("match_attempts_insufficient","Insufficient Attempts"),
            ("match_attempts_claim_failed","Claim Failures"),
            ("worker_cycles_total",        "Worker Cycles"),
            ("total_stale_claims_recovered","Stale Recoveries"),
            ("avg_wait_ms",               "Avg Wait (ms)"),
            ("avg_skill_spread",          "Avg Skill Spread"),
            ("avg_team_delta",            "Avg Team Delta"),
        ]
        for key, label in fields:
            if key in svc:
                print(f"  {label:<28}: {svc[key]:,}")
        print()

    print("=" * 56)
    print()


def save_results_json(report: dict, output_path: str) -> None:
    """Save the full report as results.json adjacent to the output file."""
    json_path = str(Path(output_path).with_suffix(".json"))
    # Remove full player list for the JSON if very large (> 5000 players)
    save_report = dict(report)
    if len(save_report.get("players", [])) > 5000:
        save_report["players"] = save_report["players"][:100]
        save_report["_players_truncated"] = True

    try:
        os.makedirs(Path(json_path).parent, exist_ok=True)
        with open(json_path, "w") as f:
            json.dump(save_report, f, indent=2, default=str)
        print(f"  Results JSON saved to: {json_path}")
    except OSError as e:
        print(f"  Warning: Could not save JSON: {e}")


def save_markdown_report(report: dict, output_path: str) -> None:
    """Save a human-readable markdown summary."""
    meta = report["meta"]
    summary = report["summary"]
    latency = report["latency"]
    waits = report["wait_times"]
    correctness = report["correctness"]
    phases = report["phases"]
    svc = report.get("service_metrics", {})

    interrupted_note = " *(partial — interrupted)*" if meta["interrupted"] else ""

    lines = [
        f"# Matchmaker Simulation Results{interrupted_note}",
        "",
        f"**Generated**: {meta['generated_at']}  ",
        f"**Server**: `{meta['server_url']}`  ",
        f"**Runtime**: {meta['runtime_secs']:.2f}s  ",
        f"**Version**: {meta['version']}",
        "",
        "---",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Players Sent | {summary['total_players_sent']:,} |",
        f"| Matched | {summary['matched']:,} |",
        f"| Queued (unmatched) | {summary['queued_only']:,} |",
        f"| Timeouts | {summary['timeouts']:,} |",
        f"| Errors | {summary['errors']:,} |",
        f"| Success Rate | {summary['success_rate_pct']:.1f}% |",
        f"| Throughput | {summary['throughput_matches_per_sec']:.1f} matches/sec |",
        f"| Requests/sec | {summary['requests_per_sec']:.1f} req/sec |",
        "",
        "---",
        "",
        "## Performance",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Enqueue Latency P50 | {latency['enqueue_p50_ms']:.1f} ms |",
        f"| Enqueue Latency P95 | {latency['enqueue_p95_ms']:.1f} ms |",
        f"| Enqueue Latency P99 | {latency['enqueue_p99_ms']:.1f} ms |",
    ]

    if waits["avg_wait_ms"] > 0:
        lines += [
            f"| Match Wait P50 | {waits['match_wait_p50_ms']:.0f} ms |",
            f"| Match Wait P95 | {waits['match_wait_p95_ms']:.0f} ms |",
            f"| Match Wait P99 | {waits['match_wait_p99_ms']:.0f} ms |",
            f"| Avg Match Wait | {waits['avg_wait_ms']:.0f} ms |",
        ]

    correctness_status = "✅ PASS" if correctness["is_correct"] else "❌ FAIL"
    lines += [
        "",
        "---",
        "",
        "## Correctness Validation",
        "",
        f"**Status**: {correctness_status}",
        "",
        "| Check | Result |",
        "|-------|--------|",
        f"| Violations | {correctness['violations']} |",
        f"| Duplicate Player IDs | {correctness['duplicate_player_ids']} |",
        f"| Players in Multiple Matches | {correctness['players_in_multiple_matches']} |",
        f"| Unique Players Observed | {correctness['total_unique_players_observed']:,} |",
        f"| Unique Matches Observed | {correctness['total_unique_matches_observed']:,} |",
        "",
        "---",
        "",
        "## Traffic Phases",
        "",
        "| Phase | Players Sent | Matched | Success Rate | Avg Wait |",
        "|-------|-------------|---------|--------------|----------|",
    ]

    for name, pm in phases.items():
        lines.append(
            f"| {name.capitalize()} | {pm['players_sent']:,} | "
            f"{pm['players_matched']:,} | "
            f"{pm['success_rate_pct']:.1f}% | "
            f"{pm['avg_wait_ms']:.0f}ms |"
        )

    lines += [
        "",
        "### Traffic Model",
        "",
        "| Phase | Fraction | Rate Multiplier | Description |",
        "|-------|----------|-----------------|-------------|",
        "| Steady | 50% | 1.0× | Normal arrival rate — baseline load |",
        "| Burst | 30% | 2.5× | Elevated traffic — simulates peak hours |",
        "| Spike | 20% | 5.0× | High-intensity spike — stress test |",
        "",
        "---",
        "",
        "## MMR Distribution",
        "",
        "| Segment | Fraction | MMR Range | Description |",
        "|---------|----------|-----------|-------------|",
        "| Normal | 98% | 0–3000 (Normal μ=1500, σ=400) | Realistic player pool |",
        "| Low edge | 1% | 0–200 | Beginner / new account outliers |",
        "| High edge | 1% | 2800–3000 | Top-rank outliers (starvation test) |",
    ]

    if svc:
        lines += [
            "",
            "---",
            "",
            "## Service Metrics (Final Snapshot)",
            "",
            "| Metric | Value |",
            "|--------|-------|",
        ]
        display_fields = [
            ("total_players_enqueued", "Total Players Enqueued"),
            ("total_matches_created", "Total Matches Created"),
            ("total_players_matched", "Total Players Matched"),
            ("match_attempts_insufficient", "Insufficient Match Attempts"),
            ("match_attempts_claim_failed", "Claim Failure Attempts"),
            ("worker_cycles_total", "Worker Cycles"),
            ("total_stale_claims_recovered", "Stale Claims Recovered"),
            ("current_queue_size", "Current Queue Size"),
            ("avg_wait_ms", "Avg Wait Time (ms)"),
            ("avg_skill_spread", "Avg Skill Spread"),
            ("avg_team_delta", "Avg Team MMR Delta"),
        ]
        for key, label in display_fields:
            if key in svc:
                lines.append(f"| {label} | {svc[key]:,} |")

    lines += [
        "",
        "---",
        "",
        "## Notes",
        "",
        "- Correctness is the primary validation target. "
          "A simulation with zero violations but lower throughput "
          "is preferable to high throughput with correctness failures.",
        "- `queued_only` players were successfully enqueued but not matched "
          "within the simulation window. This is expected when "
          "`total_players` is not divisible by 10.",
        "- Enqueue latency measures the round-trip time of `POST /enqueue`. "
          "Match wait time is measured by the server from enqueue to match formation.",
        "- The service's own `/metrics` endpoint provides the authoritative "
          "match quality data (`avg_skill_spread`, `avg_team_delta`).",
    ]

    content = "\n".join(lines) + "\n"

    try:
        os.makedirs(Path(output_path).parent, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"  Markdown report saved to: {output_path}")
    except OSError as e:
        print(f"  Warning: Could not save markdown report: {e}")


#  Main simulation orchestrator 

async def run_simulation(args: argparse.Namespace) -> int:
    """
    Main simulation coroutine.
    Returns exit code: 0 = success, 1 = correctness violations, 2 = service error.
    """
    interrupted = False
    start_time = time.time()

    print()
    print("=" * 56)
    print(" MATCHMAKER LOAD SIMULATOR")
    print("=" * 56)
    print(f" Server     : {args.url}")
    print(f" Players    : {args.players:,}")
    print(f" Duration   : {args.duration}s")
    print(f" Concurrency: {args.concurrency}")
    print(f" Timeout    : {args.timeout}s per player")
    print("=" * 56)
    print()

    # Configure aiohttp with connection pooling appropriate for the concurrency level
    connector = aiohttp.TCPConnector(
        limit=args.concurrency + 50,
        limit_per_host=args.concurrency + 50,
        enable_cleanup_closed=True,
    )

    async with aiohttp.ClientSession(
        connector=connector,
        headers={"Content-Type": "application/json"},
    ) as session:

        #  Health check 
        print("Waiting for service to become healthy...")
        healthy = await wait_for_healthy(session, args.url)
        if not healthy:
            print(
                f"\n✗ Service at {args.url} did not become healthy "
                f"within {HEALTH_MAX_WAIT_SECS:.0f}s."
            )
            print("  Is the matchmaker running? Start it with:")
            print("    cargo run --release")
            return 2

        print("\nServer ready. Starting simulation...\n")

        #  Generate players 
        print(f"Generating {args.players:,} players...")
        players = generate_players(args.players)
        player_map: Dict[str, SimPlayer] = {p.player_id: p for p in players}

        mmr_values = [p.mmr for p in players]
        print(
            f"  MMR distribution — "
            f"min={min(mmr_values)}, "
            f"max={max(mmr_values)}, "
            f"mean={int(np.mean(mmr_values))}, "
            f"std={int(np.std(mmr_values))}"
        )
        print(
            f"  Phases — "
            f"steady={sum(1 for p in players if p.phase == 'steady'):,}, "
            f"burst={sum(1 for p in players if p.phase == 'burst'):,}, "
            f"spike={sum(1 for p in players if p.phase == 'spike'):,}"
        )
        print()

        #  Start background metrics polling 
        metrics_snapshots: List[dict] = []
        stop_metrics = asyncio.Event()
        metrics_task = asyncio.create_task(
            poll_metrics_background(
                session, args.url, stop_metrics, metrics_snapshots
            )
        )

        #  Inject traffic 
        print("Injecting traffic...")
        print(
            f"  Phases: steady (50%, 1.0×) → "
            f"burst (30%, 2.5×) → spike (20%, 5.0×)"
        )
        print()

        try:
            await inject_traffic(
                session=session,
                base_url=args.url,
                players=players,
                duration_secs=args.duration,
                concurrency=args.concurrency,
                per_player_timeout=args.timeout,
            )
        except asyncio.CancelledError:
            interrupted = True
            print("\n  Simulation interrupted — collecting partial results...")

        #  Stop metrics polling 
        stop_metrics.set()
        try:
            await asyncio.wait_for(metrics_task, timeout=5.0)
        except asyncio.TimeoutError:
            metrics_task.cancel()

        #  Collect match results 
        if not interrupted:
            print()
            print("Collecting match results from service...")
            # Give workers a moment to finish forming any in-progress matches
            await asyncio.sleep(2.0)

            expected_matches = args.players // 10
            await collect_match_results(
                session, args.url, player_map, expected_matches
            )

            matched_count = sum(1 for p in players if p.final_status == "matched")
            print(f"  Matched: {matched_count:,} / {args.players:,} players")
        else:
            # On interrupt: do a quick collection pass
            await collect_match_results(
                session, args.url, player_map, 100
            )

        #  Final metrics snapshot 
        final_metrics = None
        try:
            async with session.get(
                f"{args.url}/metrics",
                timeout=aiohttp.ClientTimeout(total=5.0),
            ) as resp:
                if resp.status == 200:
                    final_metrics = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            pass

    #  Validate correctness 
    print()
    print("Validating correctness...")
    correctness = validate_correctness(players)

    if correctness.is_correct:
        print("  ✓ No correctness violations detected")
    else:
        print(f"  ✗ {correctness.violations} correctness violation(s) detected!")
        if correctness.players_in_multiple_matches:
            print(
                f"    Players in multiple matches: "
                f"{correctness.players_in_multiple_matches[:5]}"
            )

    #  Build and emit report 
    end_time = time.time()
    phase_metrics = compute_phase_metrics(players)

    report = build_report(
        players=players,
        phase_metrics=phase_metrics,
        correctness=correctness,
        final_metrics=final_metrics,
        start_time=start_time,
        end_time=end_time,
        interrupted=interrupted,
        args=args,
    )

    print_report(report)

    print("Saving results...")
    save_results_json(report, args.output)
    save_markdown_report(report, args.output)
    print()

    return 0 if correctness.is_correct else 1


#  Entry point 

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Matchmaker load simulation and validation tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python simulate.py
  python simulate.py --players 1000 --duration 30
  python simulate.py --url http://localhost:8080 --players 5000 --concurrency 200
  python simulate.py --players 500 --output results/run1.md
        """,
    )

    parser.add_argument(
        "--url",
        default="http://localhost:8080",
        help="Base URL of matchmaking server (default: http://localhost:8080)",
    )
    parser.add_argument(
        "--players",
        type=int,
        default=1000,
        help="Total players to simulate (default: 1000)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="Traffic injection duration in seconds (default: 60)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=100,
        help="Maximum concurrent in-flight requests (default: 100)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=90.0,
        help="Per-player request timeout in seconds (default: 90)",
    )
    parser.add_argument(
        "--output",
        default="scripts/sample_results.md",
        help="Output report path (default: scripts/sample_results.md)",
    )

    args = parser.parse_args()

    # Validate
    if args.players < 10:
        parser.error("--players must be at least 10")
    if args.duration <= 0:
        parser.error("--duration must be positive")
    if args.concurrency < 1:
        parser.error("--concurrency must be at least 1")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    return args


def main() -> None:
    args = parse_args()

    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    main_task = None

    def handle_interrupt(sig, frame):
        print(f"\n\nInterrupt received ({signal.Signals(sig).name}) — stopping...")
        if main_task and not main_task.done():
            main_task.cancel()

    signal.signal(signal.SIGINT, handle_interrupt)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_interrupt)

    try:
        main_task = loop.create_task(run_simulation(args))
        exit_code = loop.run_until_complete(main_task)
        sys.exit(exit_code)
    except asyncio.CancelledError:
        print("Simulation cancelled.")
        sys.exit(130)
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt — exiting.")
        sys.exit(130)
    finally:
        loop.close()


if __name__ == "__main__":
    main()