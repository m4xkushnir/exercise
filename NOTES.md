# Notes

## Part 1: Incidents

| Incident | Root cause | Reproducing test | Fix |
|----------|------------|------------------|-----|
| INC-1 | | | |
| INC-2 | | | |
| INC-3 | | | |
| INC-4 | | | |

## Part 2: Performance

Machine is a MacBook Air with an Intel i5-1030NG7, 4 cores and 8 threads, so the 16 thread bench is oversubscribed 2x.

| Bench | Before | After |
|-------|--------|-------|
| acquire_release/1  | 3.53 µs, 284 K/s | |
| acquire_release/4  | 5.80 µs, 690 K/s | |
| acquire_release/16 | 16.2 µs, 985 K/s | |

Changes, and why each one helps (or didn't):

-

Selection policy, and why:

## Part 3: Waiting for a slot

## Part 4: Global admission across 6 pods

## Bonus: INC-5 and INC-6

## Before shipping, I would still want

-
