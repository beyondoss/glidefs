---
name: audit-testing
description: Audit test coverage, quality, and gaps for a codebase area. Identifies missing critical tests, evaluates test patterns, and scores gaps with ICE model. Use when asked about test coverage, missing tests, or testing quality.
allowed-tools: Read, Glob, Grep, Bash, LSP
---

# Testing Audit

## Subagent Policy

When spawning Task subagents to read files (e.g., for parallel codebase exploration), always use `model: "sonnet"`. Reserve opus for the final synthesis and judgment.

You are auditing the testing posture of a codebase area. Your job is to find the tests that are MISSING — the ones that would have caught the bug that ships at 2am on a Friday. Prioritize ruthlessly.

## Step 1: Detect Persona

Use the same persona auto-detection as the `audit` skill based on target directory language. A rustacean evaluates `#[cfg(test)]` modules and integration tests differently than a gopher evaluates `_test.go` files.

| Target          | Language | Test ecosystem                                             |
| --------------- | -------- | ---------------------------------------------------------- |
| `boxes/`        | Rust     | `#[cfg(test)]`, `/tests/`, mock traits, test harnesses     |
| `edge/`         | Rust     | `#[cfg(test)]`, `/tests/`, mock store, test certificates   |
| `tunnel/`       | Rust     | `#[cfg(test)]`, `/tests/`, mock certs, integration tests   |
| `orchestrator/` | Rust     | `#[cfg(test)]`, mock traits                                |
| `rustlib/`      | Rust     | `#[cfg(test)]`, fixture-based schema validation            |
| `api/`          | Go       | `_test.go`, build tags, seeder pattern, `testutil` package |
| `cli/`          | Go       | `_test.go`, integration tests                              |
| `web/`          | TS/React | Jest/Vitest, React Testing Library, component tests        |

## Step 2: Pre-Work

1. **Read `CLAUDE.md`** — note the idempotency/atomicity requirements (these MUST be tested)
2. **Inventory all test files**:
   - Rust: `Grep` for `#[cfg(test)]` and `#[test]`, `Glob` for `tests/*.rs`
   - Go: `Glob` for `*_test.go`, check for `//go:build integration` tags
   - TS: `Glob` for `*.test.ts`, `*.spec.ts`
4. **Read `api/pkg/testutil/ARCHITECTURE.md`** if auditing Go code (understand seeder pattern)
5. **Check for wire compat tests** if the target consumes NATS messages: look for `wire_compat_test.go`
6. **Read the testing infrastructure**: mock traits, test harnesses, fixtures

## Step 3: Evaluate Test Quality

### Test Taxonomy

Classify existing tests into these categories:

| Category          | What it tests                                | Example                         |
| ----------------- | -------------------------------------------- | ------------------------------- |
| **Unit**          | Single function/method, mocked dependencies  | State machine transitions       |
| **Integration**   | Multiple components with real dependencies   | Service + repo + real Postgres  |
| **Wire compat**   | Go/Rust schema agreement via JSON fixtures   | `wire_compat_test.go`           |
| **Property/fuzz** | Invariants across random inputs              | Fuzz config parsing             |
| **Benchmark**     | Performance characteristics                  | `bench_test.go`, `#[bench]`     |
| **E2E**           | Full system flow                             | Relay e2e test                  |
| **Idempotency**   | Operation safe to retry                      | Create-if-not-exists            |
| **State machine** | All transitions, guards, invalid transitions | FSM transition table coverage   |
| **Error path**    | Failure modes, error propagation             | Network timeout, disk full      |
| **Concurrency**   | Race conditions, deadlocks                   | Parallel access to shared state |

### Evaluation Dimensions

**1. Critical Path Coverage**

- Is the happy path tested end-to-end?
- Are the top 3 most common operations tested?

**2. State Machine Coverage**

- Are ALL states reachable in tests?
- Are ALL valid transitions tested?
- Are INVALID transitions tested (should error/panic)?
- Does test coverage match the transition table in ARCHITECTURE.md?

**3. Error Path Coverage**

- What happens when the database is down?
- What happens when NATS is unreachable?
- What happens on invalid input at every boundary?

**4. Idempotency Tests** (CLAUDE.md requirement)

- For every state-modifying operation: is there a test that runs it twice and asserts same result?
- For infrastructure ops (ZFS, OVN, iptables): are "already exists" and "not found" paths tested?

**5. Concurrency Tests**

- Are shared data structures tested under concurrent access?
- Are there tests for goroutine/task cancellation?

**6. Wire Compatibility** (for NATS consumers)

- Does every Go consumer have a `wire_compat_test.go`?
- Do fixture tests cover all fields?

**7. Test Quality Smells**

- Tests with no assertions
- Tests that depend on execution order
- Tests with sleeps instead of synchronization
- Tests with excessive mocking (testing the mocks, not the code)

## Step 4: Score with ICE

ICE is a **prioritization framework**, not a severity assessment. The question is never "is this worth doing vs. doing nothing?" — there is always work to do. The question is "what test should we write next?" A low-impact, high-confidence, high-ease gap is a legitimate quick win that belongs high in the priority list. Do not editorialize over the scores. Trust the framework — if a score feels wrong, fix the individual dimension scores, don't override the result.

| Dimension      | Scale | Description                                               |
| -------------- | ----- | --------------------------------------------------------- |
| **Impact**     | 1-10  | How bad would the untested failure mode be in production? |
| **Confidence** | 1-10  | How sure are you this test is actually missing?           |
| **Ease**       | 1-10  | How easy is this test to write? (10 = trivial)            |
| **ICE Score**  |       | (Impact + Confidence + Ease) / 3                          |

### Impact Calibration for Tests

- 1-2: Style/convention test
- 3-4: Catches minor regressions in non-critical paths
- 5-6: Catches regressions in important but not critical paths
- 7-8: Catches bugs that would cause user-visible errors or data inconsistency
- 9-10: Catches bugs that would cause data loss, security breach, or extended outage

### Composite Score

- **8.0–10.0**: Do it now — high-ROI, no reason to wait
- **6.0–7.9**: Do it soon — meaningful improvement, plan it in
- **4.0–5.9**: Backlog — worth doing when time allows
- **Below 4.0**: Ignore unless it compounds with other issues

## Step 5: Output Format

````
## Testing Audit: {target directory}

**Persona**: {persona}
**Scope**: {what was examined}

---

## Overall: {X}/10

{2-3 sentence summary. "Has solid unit tests but integration tests are thin and idempotency is untested."}

## Ratings

| Category | Rating | Notes |
| --- | --- | --- |
| Critical Path Coverage | {X}/10 | {one-line} |
| State Machine Coverage | {X}/10 | {one-line} |
| Error Path Coverage | {X}/10 | {one-line} |
| Idempotency Tests | {X}/10 | {one-line} |
| Concurrency Tests | {X}/10 | {one-line} |
| Wire Compatibility | {X}/10 | {one-line} |
| Test Quality | {X}/10 | {one-line} |

Skip categories that don't apply (e.g. should only use Wire Compatibility for Go)

## Test Inventory

| Category | Count | Key files |
| --- | --- | --- |
| Unit | {n} | {files} |
| Integration | {n} | {files} |
| Wire compat | {n} | {files} |
| Benchmark | {n} | {files} |
| E2E | {n} | {files} |

---

## What I Like

{Specific things the tests do well. File references. Not generic — cite the exact test pattern, helper, or coverage choice.}

- **{Merit}** — `{file:line}`. {Why this is good.}
- ...

---

## What Concerns Me

{Missing tests and test quality issues. Compact ICE. Only suggest what to write for clear gaps.}

### {Missing test or quality issue}
`{file or area}` · {type} · ICE {I}/{C}/{E} → {score} · {trivial | moderate | significant}

{What's missing and what production failure it would catch. 2-4 sentences.}

**Sketch** (only for high-ICE items):
```{language}
// Brief test skeleton — 5-10 lines max
````

---

## Concerns Summary

| # | Gap           | Type                   | Effort   | ICE   |
| - | ------------- | ---------------------- | -------- | ----- |
| 1 | {description} | {unit/integration/...} | {effort} | {n.n} |
| 2 | {description} | {type}                 | {effort} | {n.n} |

## Quick Wins

{Top 3 tests that add the most confidence with least effort.}

```
## Calibration Rules

- **Stack rank honestly.** The #1 missing test should catch the most dangerous bug.
- **Do not list more than 15 gaps.** Keep the top 15 by ICE score.
- **Every gap must be specific.** Not "add more error tests" but "test that `CreateVM` returns `AlreadyExists` when called twice with the same ID."
- **Sketches only for high-ICE items.** Don't pad the output with boilerplate test skeletons for every gap.
- **Wire compat tests are non-negotiable.** If a Go consumer lacks `wire_compat_test.go`, that is always high-ICE.
```
