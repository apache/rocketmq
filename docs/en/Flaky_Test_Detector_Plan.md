# RocketMQ Flaky Test Detection Plan

## Background & Goals

RocketMQ's mainline CI frequently experiences intermittent test failures, causing developer trust fatigue toward red builds and masking real regressions. This plan uses large-scale repeated execution to statistically measure failure rates, marks unstable methods (≥1%) with `@Ignore` to restore CI reliability, and retains the data to prioritize subsequent fixes.

## Methodology References

- **Google** — [Flaky Tests at Google and How We Mitigate Them](https://testing.googleblog.com/2016/05/flaky-tests-at-google-and-how-we.html) (2016): Introduced "deflake" (run N times to measure failure rate) and "quarantine" (isolate flaky tests from mainline CI). Internal data: ~1.5% of tests are flaky, 16% have been flaky at some point.
- **Meta** — [Predictive Test Selection](https://engineering.fb.com/2018/11/21/developer-tools/predictive-test-selection/) (2018): Uses aggressive retry to separate flaky failures from real regressions.
- **Spotify** — [Test Flakiness Methods](https://engineering.atspotify.com/2019/11/test-flakiness-methods-for-identifying-and-dealing-with-flaky-tests/) (2019): Three-stage framework of repeated execution + isolation + tracking.

## Core Idea: Three-Layer Funnel

A "coarse → fine → pinpoint" strategy to progressively narrow scope and avoid wasting compute at the full method level:

```
Layer 1: Module level (16 modules × 100 runs) → filter out modules with failures
Layer 2: Class level (only classes in unstable modules × 100 runs) → filter out classes with failures
Layer 3: Method level (only methods in unstable classes × 100 runs) → precisely locate each method's failure rate
```

After each layer, Surefire XML reports are analyzed and the unstable list feeds the next layer. After marking, a full re-run verifies stability; if new flaky tests surface, the mark + verify cycle repeats until zero failures.

## Execution Architecture

- **Control node (local)**: Orchestrates task distribution, result collection, data analysis
- **Worker nodes (10 ECS, 16C 64G each)**: Max 4 Docker containers per node in parallel, each test run isolated

## Execution Flow

```
1. Build      → Compile RocketMQ with JDK 8 inside Docker, package as test image
2. Distribute → Relay image via internal network to all worker nodes
3. Dispatch   → Generate task list, split evenly across nodes, start workers
4. Collect    → Poll until complete, retrieve Surefire XML reports
5. Analyze    → Parse XML, compute failure count and rate per method
6. Mark       → Add @Ignore to methods exceeding threshold
7. Verify     → Rebuild and run full suite to confirm trunk stability
```

## Key Design Decisions

| Decision Point | Choice | Rationale |
|---------------|--------|-----------|
| Build environment | JDK 8 inside Docker | Local JDK versions vary; container ensures consistency |
| Image distribution | Upload to one node, relay via internal network | Internal bandwidth far exceeds public internet |
| Test isolation | Independent container per run | Avoids residual processes, port conflicts |
| Failure threshold | ≥1% failure rate | ~10 failures across 1000 effective runs; balances false positives vs. missed cases |
| Marking approach | `@Ignore` + failure rate comment | Minimal intrusion, easy to re-enable later |
| Verification loop | Full re-run after marking | Handles "hidden flaky" problem |

## Follow-up Plan

- Prioritize root-cause analysis and fix for high failure rate methods (>10%); remove `@Ignore` and re-verify after fix
- Consider integrating the detection tool into periodic CI tasks for continuous stability monitoring
