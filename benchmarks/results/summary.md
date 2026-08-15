# Benchmark summary: main vs this branch

- JVM: openjdk version "21.0.12" 2026-07-21 LTS, flags `-Xms512m -Xmx512m`
- OS: Darwin arm64, date 2026-08-15T06:30:57Z
- Trials: 5 per side, interleaved (fresh JVM per trial). Values are medians.
- Before = `main` (c04e37b), after = this branch (de5f041).
- Delta = (after - before) / before. Positive throughput delta is better;
  positive latency/memory delta is worse. Deltas within the run-to-run noise
  band (max spread across trials of either side) are marked `~` (equivalent).

## offer() throughput

| Case | Metric | main | this branch | delta |
|---|---|---:|---:|---:|
| 64 B, checksum on | msgs_per_sec (msgs/s) | 13,444 | 11,362 | -15.5% ~ |
| 64 B, checksum on | mib_per_sec (MiB/s) | 0.8 | 0.7 | -15.5% ~ |
| 64 B, checksum on | alloc_bytes_per_op (B/op) | 298 | 298 | +0.3% |
| 64 B, checksum on | peak_rss_bytes (bytes) | 87.8 MiB | 88.6 MiB | +0.9% ~ |
| 1 KiB, checksum on | msgs_per_sec (msgs/s) | 12,050 | 12,592 | +4.5% ~ |
| 1 KiB, checksum on | mib_per_sec (MiB/s) | 11.8 | 12.3 | +4.5% ~ |
| 1 KiB, checksum on | alloc_bytes_per_op (B/op) | 3,178 | 3,178 | +0.0% |
| 1 KiB, checksum on | peak_rss_bytes (bytes) | 160.1 MiB | 171.1 MiB | +6.9% |
| 1 KiB, checksum off | msgs_per_sec (msgs/s) | 13,483 | 12,917 | -4.2% ~ |
| 1 KiB, checksum off | mib_per_sec (MiB/s) | 13.2 | 12.6 | -4.2% ~ |
| 1 KiB, checksum off | alloc_bytes_per_op (B/op) | 3,178 | 3,178 | +0.0% |
| 1 KiB, checksum off | peak_rss_bytes (bytes) | 160.1 MiB | 171.6 MiB | +7.2% |
| 8 KiB, checksum on | msgs_per_sec (msgs/s) | 11,784 | 12,021 | +2.0% ~ |
| 8 KiB, checksum on | mib_per_sec (MiB/s) | 92.1 | 93.9 | +2.0% ~ |
| 8 KiB, checksum on | alloc_bytes_per_op (B/op) | 24,682 | 24,682 | +0.0% |
| 8 KiB, checksum on | peak_rss_bytes (bytes) | 265.9 MiB | 276.7 MiB | +4.1% |
| 64 KiB, checksum on | msgs_per_sec (msgs/s) | 7,129 | 6,785 | -4.8% ~ |
| 64 KiB, checksum on | mib_per_sec (MiB/s) | 445.6 | 424.1 | -4.8% ~ |
| 64 KiB, checksum on | alloc_bytes_per_op (B/op) | 196,714 | 196,715 | +0.0% |
| 64 KiB, checksum on | peak_rss_bytes (bytes) | 360.7 MiB | 360.8 MiB | +0.0% ~ |

## poll() throughput

| Case | Metric | main | this branch | delta |
|---|---|---:|---:|---:|
| 64 B, checksum on | msgs_per_sec (msgs/s) | 214 | 201 | -6.5% ~ |
| 64 B, checksum on | mib_per_sec (MiB/s) | 0.0 | 0.0 | -6.5% ~ |
| 64 B, checksum on | alloc_bytes_per_op (B/op) | 713 | 817 | +14.6% |
| 64 B, checksum on | heap_after_cycle_bytes (bytes) | 1.3 MiB | 1.3 MiB | +1.6% |
| 64 B, checksum on | peak_rss_bytes (bytes) | 97.5 MiB | 98.0 MiB | +0.6% ~ |
| 1 KiB, checksum on | msgs_per_sec (msgs/s) | 217 | 220 | +1.3% ~ |
| 1 KiB, checksum on | mib_per_sec (MiB/s) | 0.2 | 0.2 | +1.3% ~ |
| 1 KiB, checksum on | alloc_bytes_per_op (B/op) | 5,513 | 5,633 | +2.2% |
| 1 KiB, checksum on | heap_after_cycle_bytes (bytes) | 1.3 MiB | 1.3 MiB | +1.6% |
| 1 KiB, checksum on | peak_rss_bytes (bytes) | 111.4 MiB | 111.9 MiB | +0.4% ~ |
| 1 KiB, checksum off | msgs_per_sec (msgs/s) | 208 | 201 | -3.2% ~ |
| 1 KiB, checksum off | mib_per_sec (MiB/s) | 0.2 | 0.2 | -3.2% ~ |
| 1 KiB, checksum off | alloc_bytes_per_op (B/op) | 5,513 | 5,633 | +2.2% |
| 1 KiB, checksum off | heap_after_cycle_bytes (bytes) | 1.3 MiB | 1.3 MiB | +1.6% |
| 1 KiB, checksum off | peak_rss_bytes (bytes) | 111.2 MiB | 111.8 MiB | +0.5% ~ |
| 8 KiB, checksum on | msgs_per_sec (msgs/s) | 202 | 201 | -0.5% ~ |
| 8 KiB, checksum on | mib_per_sec (MiB/s) | 1.6 | 1.6 | -0.5% ~ |
| 8 KiB, checksum on | alloc_bytes_per_op (B/op) | 41,353 | 41,473 | +0.3% |
| 8 KiB, checksum on | heap_after_cycle_bytes (bytes) | 1.3 MiB | 1.3 MiB | +1.5% |
| 8 KiB, checksum on | peak_rss_bytes (bytes) | 157.3 MiB | 148.9 MiB | -5.3% |
| 64 KiB, checksum on | msgs_per_sec (msgs/s) | 208 | 217 | +4.2% ~ |
| 64 KiB, checksum on | mib_per_sec (MiB/s) | 13.0 | 13.5 | +4.2% ~ |
| 64 KiB, checksum on | alloc_bytes_per_op (B/op) | 328,073 | 328,193 | +0.0% |
| 64 KiB, checksum on | heap_after_cycle_bytes (bytes) | 1.3 MiB | 1.4 MiB | +1.5% |
| 64 KiB, checksum on | peak_rss_bytes (bytes) | 379.5 MiB | 380.2 MiB | +0.2% ~ |

## Round-trip latency (offer+poll)

| Case | Metric | main | this branch | delta |
|---|---|---:|---:|---:|
| 1 KiB, checksum on | p50_us (us) | 8,692.0 | 8,576.8 | -1.3% ~ |
| 1 KiB, checksum on | p95_us (us) | 10,375.9 | 12,470.1 | +20.2% ~ |
| 1 KiB, checksum on | p99_us (us) | 11,827.3 | 17,036.0 | +44.0% ~ |
| 1 KiB, checksum on | alloc_bytes_per_op (B/op) | 8,786 | 8,954 | +1.9% |

## Open+close on populated file (5,000 x 1 KiB messages)

| Case | Metric | main | this branch | delta |
|---|---|---:|---:|---:|
| 1 KiB, checksum on | p50_us (us) | 8,634.8 | 9,364.5 | +8.5% ~ |
| 1 KiB, checksum on | p95_us (us) | 11,391.7 | 12,438.0 | +9.2% ~ |
| 1 KiB, checksum on | mean_us (us) | 8,991.4 | 9,700.3 | +7.9% ~ |

`~` = within run-to-run noise; treat as equivalent.
