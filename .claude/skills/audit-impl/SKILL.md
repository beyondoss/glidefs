---
name: audit-impl
description: Self-audit after implementing a plan. Verifies completeness, correctness, and that everything is wired up. Rates confidence 1-10 and keeps working until confidence reaches 9+. Use after completing a plan, feature, or significant change.
allowed-tools: Read, Glob, Grep, Bash, LSP, Edit, Write
model: claude-sonnet-4-6
---

# Implementation Audit

## Subagent Policy

When spawning Task subagents to read files (e.g., for parallel codebase exploration), always use `model: "haiku"`. Reserve opus for the final synthesis and judgment.

You just finished implementing something. Now prove it works. This is not a victory lap — this is the moment you catch the thing that would have broken at demo time.

**You may not stop working until your confidence has reached at least 9/10.** The 1-point deduction is reserved for things you genuinely cannot know (runtime behavior you can't test locally, edge cases in production traffic, third-party service behavior). It is NOT for laziness, shortcuts, or "I think it's probably fine."

## What This Audit Is

This is a completeness and correctness check against the goals that were set. It answers:

- Did we do what we said we'd do?
- Is the code actually wired up, or did we write functions nobody calls?
- Are there TODOs, placeholders, or half-finished paths?
- Will this "just work" when someone runs it?

## Step 1: Establish the Goals

Before auditing, clearly state what was supposed to be accomplished. Sources:

1. **The plan file** — if a plan was written, read it. Every goal in the plan is a checkbox.
2. **The conversation history** — what did the user ask for?
3. **The commit messages** — what did we claim to have done?

Write out the goals as a numbered list. Every goal must be verified.

## Step 2: Verify Each Goal

For each goal, check:

### Completeness

- Is the feature/fix fully implemented, not partially?
- Are all code paths handled (not just the happy path)?
- Are all files that needed changing actually changed?
- Are all new files properly imported/registered/wired up?

### No Loose Ends

- **Search for TODOs**: `Grep` for `TODO`, `FIXME`, `HACK`, `XXX`, `PLACEHOLDER` in all files you touched
- **Search for dead code**: Functions, structs, or constants you wrote that nothing calls
- **Search for incomplete error handling**: `unwrap()`, `expect()`, `panic!()` in Rust; unchecked errors in Go; unhandled promise rejections in TS — that shouldn't be there
- **Search for commented-out code**: Code you disabled "temporarily" and forgot to remove or re-enable
- **Search for placeholder values**: Hardcoded strings, magic numbers, dummy data that should be real

### Wiring

- If you added a new endpoint, is it registered in the router?
- If you added a new NATS handler, is it subscribed?
- If you added a new config field, is it loaded and used?
- If you added a migration, is it in the correct sequence?
- If you added a new file, is it included in `mod.rs` / imported where needed?
- If you added a new dependency, is it in `Cargo.toml` / `go.mod` / `package.json`?

### Correctness

- Does the code actually do what the goal says, or does it do something adjacent?
- Are the types right? (Not `String` where you need `usid::ID`, not `i32` where you need `i64`)
- Are the error messages helpful? (Not generic "operation failed")
- Is the code consistent with patterns in the rest of the codebase?

### Build & Compile

- Does linting pass? `mise lint:[service]`
- Are there warnings? Warnings often indicate real issues.
- Do tests pass? `mise test:[integration|unit]:[service]`

## Step 3: Rate Your Confidence

| Score | Meaning                                                                                                                                                                        | Action                   |
| ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------ |
| 10    | Perfect. I verified every goal, everything compiles, tests pass, no loose ends.                                                                                                | Done.                    |
| 9     | Confident. Everything is wired up and correct. The 1-point deduction is for things I genuinely cannot verify (runtime behavior, production edge cases, external dependencies). | Done.                    |
| 8     | Almost there. I found 1-2 minor issues I can fix right now.                                                                                                                    | Fix them, then re-audit. |
| 7     | Mostly done. There are gaps I can identify and address.                                                                                                                        | Fix them, then re-audit. |
| 6     | Significant gaps. Multiple issues need attention.                                                                                                                              | Fix them, then re-audit. |
| 1-5   | Not done. Major goals unmet or code won't compile.                                                                                                                             | Keep working.            |

**If your confidence is below 9: fix the issues you found, then run this audit again.** Do not inflate your rating to stop working. Do not claim 9 if you haven't actually verified the goals.

## Step 4: Output Format

```
## Implementation Audit

### Goals
1. {goal from plan/conversation}
2. {goal}
3. {goal}

### Verification

#### Goal 1: {goal}
**Status**: {DONE | PARTIAL | MISSING}
{What was implemented. If PARTIAL/MISSING, what's left.}

#### Goal 2: {goal}
**Status**: {DONE | PARTIAL | MISSING}
{What was implemented. If PARTIAL/MISSING, what's left.}

...

### Loose Ends Check
- **TODOs found**: {count} — {list with file:line, or "none"}
- **Dead code**: {any functions/types written but never called, or "none"}
- **Incomplete error handling**: {any unwrap/panic/unchecked errors, or "none"}
- **Commented-out code**: {any, or "none"}
- **Placeholder values**: {any hardcoded/dummy values, or "none"}

### Wiring Check
- {each new thing and whether it's properly registered/imported/connected}

### Build Status
- **Compiles**: {yes/no}
- **Warnings**: {count and summary, or "none"}
- **Tests**: {pass/fail/not run, with details}

---

## Confidence: {X}/10

{1-2 sentence justification. If 9+, state what the 1-point deduction is for. If <9, state exactly what needs to be fixed.}
```

### If Confidence < 9

List the specific issues, fix them, then output a new audit:

```
### Issues Found (fixing now)
1. {issue} — {fix}
2. {issue} — {fix}

{... fix the issues ...}

## Re-Audit After Fixes

{Run through the same verification again}

## Updated Confidence: {X}/10
{justification}
```

Repeat until confidence reaches 9.

## Honesty Rules

- **Do not round up.** If you're at 8, you're at 8. Fix the issues.
- **Do not claim "just work" without verifying.** If you haven't checked the wiring, you don't know it works.
- **The build must pass.** If it doesn't compile, your confidence cannot be above 5.
- **TODOs are not acceptable at 9+.** If you wrote a TODO, either do the TODO or remove it with a comment explaining why it's deferred.
- **"I think it's fine" is not 9.** 9 means "I verified it's fine."
- **The 1-point deduction is for genuine unknowns**, not for things you could have checked but didn't. Examples of legitimate 1-point deductions:
  - "Can't verify NATS message delivery without a running NATS server"
  - "Can't test the migration against production data volume"
  - "Can't verify the UI renders correctly without running the full dev server"
  - "Edge case behavior under high concurrency would need load testing"
