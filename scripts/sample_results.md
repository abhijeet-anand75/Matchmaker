# Matchmaker Simulation Results

**Generated**: 2026-06-02T11:50:16.089175Z  
**Server**: `http://localhost:8080`  
**Runtime**: 62.47s  
**Version**: 1.0.0

---

## Summary

| Metric | Value |
|--------|-------|
| Players Sent | 5,000 |
| Matched | 4,830 |
| Queued (unmatched) | 170 |
| Timeouts | 0 |
| Errors | 0 |
| Success Rate | 96.6% |
| Throughput | 7.7 matches/sec |
| Requests/sec | 80.0 req/sec |

---

## Performance

| Metric | Value |
|--------|-------|
| Enqueue Latency P50 | 1.1 ms |
| Enqueue Latency P95 | 2.3 ms |
| Enqueue Latency P99 | 4.0 ms |
| Match Wait P50 | 8906 ms |
| Match Wait P95 | 13780 ms |
| Match Wait P99 | 14698 ms |
| Avg Match Wait | 8573 ms |

---

## Correctness Validation

**Status**: ✅ PASS

| Check | Result |
|-------|--------|
| Violations | 0 |
| Duplicate Player IDs | 0 |
| Players in Multiple Matches | 0 |
| Unique Players Observed | 5,000 |
| Unique Matches Observed | 483 |

---

## Traffic Phases

| Phase | Players Sent | Matched | Success Rate | Avg Wait |
|-------|-------------|---------|--------------|----------|
| Steady | 2,500 | 2,406 | 96.2% | 8581ms |
| Burst | 1,500 | 1,451 | 96.7% | 8621ms |
| Spike | 1,000 | 973 | 97.3% | 8483ms |

### Traffic Model

| Phase | Fraction | Rate Multiplier | Description |
|-------|----------|-----------------|-------------|
| Steady | 50% | 1.0× | Normal arrival rate — baseline load |
| Burst | 30% | 2.5× | Elevated traffic — simulates peak hours |
| Spike | 20% | 5.0× | High-intensity spike — stress test |

---

## MMR Distribution

| Segment | Fraction | MMR Range | Description |
|---------|----------|-----------|-------------|
| Normal | 98% | 0–3000 (Normal μ=1500, σ=400) | Realistic player pool |
| Low edge | 1% | 0–200 | Beginner / new account outliers |
| High edge | 1% | 2800–3000 | Top-rank outliers (starvation test) |

---

## Service Metrics (Final Snapshot)

| Metric | Value |
|--------|-------|
| Total Players Enqueued | 5,000 |
| Total Matches Created | 483 |
| Total Players Matched | 4,830 |
| Insufficient Match Attempts | 9,468 |
| Claim Failure Attempts | 6 |
| Worker Cycles | 9,957 |
| Stale Claims Recovered | 0 |
| Current Queue Size | 170 |
| Avg Wait Time (ms) | 8,573 |
| Avg Skill Spread | 131 |
| Avg Team MMR Delta | 1 |

---

## Notes

- Correctness is the primary validation target. A simulation with zero violations but lower throughput is preferable to high throughput with correctness failures.
- `queued_only` players were successfully enqueued but not matched within the simulation window. This is expected when `total_players` is not divisible by 10.
- Enqueue latency measures the round-trip time of `POST /enqueue`. Match wait time is measured by the server from enqueue to match formation.
- The service's own `/metrics` endpoint provides the authoritative match quality data (`avg_skill_spread`, `avg_team_delta`).
