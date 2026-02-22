---
name: architecture-doc
description: Write or improve ARCHITECTURE.md files for packages and systems. Use when creating documentation for a new package, reviewing existing architecture docs, or when asked to document how a system works.
allowed-tools: Read, Glob, Grep, Write, Edit
model: claude-sonnet-4-6
---

# Writing Excellent Architecture Documentation

You are writing an ARCHITECTURE.md that will help engineers understand a system in 60 seconds, navigate the code in 5 minutes, and ship changes confidently in 30 minutes.

If engineers need to reverse-engineer the code to understand it, the documentation failed.

## Required Sections

Every architecture doc MUST have these sections in this order:

### 1. One-Line Summary

Start with a single sentence: what does this system do and why does it exist?

```markdown
# {Package} Architecture

{One sentence describing what this does and its purpose in the larger system.}
```

### 2. Data Flow Diagram

Show the happy path visually. ASCII art is preferred over no diagram.

```
Request ──► Auth ──► Authz ──► Rate Limit ──► Router ──► Backend
              │         │           │
            401       403         429
```

For complex systems, show multiple flows:

- Request/response flow
- Event/message flow
- State transitions

### 3. Concepts & Terminology

Define domain terms precisely. Include a "NOT" column to prevent misunderstandings.

```markdown
| Term      | Definition                           | NOT                   |
| --------- | ------------------------------------ | --------------------- |
| Namespace | Routing key extracted from subdomain | Not a tenant boundary |
| Node      | Compute machine running VMs          | Not the proxy host    |
| Address   | Socket address (IP:port) of backend  | Not a hostname        |
```

### 4. Core Mechanism

Explain the main thing this system does with enough detail to implement it. This is the heart of the document.

Include:

- The algorithm or pattern used
- Key data structures
- Important invariants
- Code snippets for complex logic

### 5. State Machine (if applicable)

If the system has states, show them as ASCII art AND a transition table:

```
pending ──Create──► creating ──Complete──► running
                        │                      │
                      Failed              StopRequested
                        │                      │
                        ▼                      ▼
                     failed ◀── StopFailed ── stopping
```

| From     | Event           | To       | Guard           |
| -------- | --------------- | -------- | --------------- |
| pending  | CreateScheduled | creating | -               |
| creating | CreateCompleted | running  | -               |
| creating | CreateFailed    | failed   | -               |
| creating | CreateTimeout   | failed   | elapsed > 10min |

### 6. Design Decisions

This section elevates documentation from "adequate" to "excellent". Explain WHY you chose X over Y.

```markdown
### Why {X} over {Y}?

We chose X because:

1. {Specific reason with tradeoff}
2. {Performance or simplicity benefit}
3. {What we tried that didn't work}

We considered Y but rejected it because:

- {Concrete limitation}
- {Doesn't fit our constraints}
```

Real example:

```markdown
### Why state machines over ad-hoc conditionals?

1. **Single source of truth** - All transition rules in one package
2. **Testable** - Pure functions with table-driven tests
3. **Auditable** - Every transition produces a record with reason
4. **Explicit** - State diagrams in docs match code exactly
```

### 7. Security / Trust Model (if applicable)

Document what the system verifies AND what it does NOT verify:

```markdown
## Trust Model

**What we verify:**

- Certificate signed by trusted CA
- Certificate not expired
- Requested resource is in valid_principals

**What we do NOT verify:**

- Client identity beyond certificate
- Authorization to access specific data
- Application-layer credentials

**Why this is acceptable:**

- Application-layer auth (database password) is the real gate
- Transport security is a separate concern from authz
```

### 8. Package Structure

Map files to responsibilities:

```markdown
| File              | Purpose                             |
| ----------------- | ----------------------------------- |
| `state/events.go` | Event types (CreateCompleted, etc.) |
| `state/status.go` | StatusMachine with transition rules |
| `repo.go`         | Data access with atomic operations  |
| `service.go`      | Business logic with saga pattern    |
```

### 9. Configuration

Document environment variables with defaults AND rationale:

```markdown
| Variable          | Default | Why                                         |
| ----------------- | ------- | ------------------------------------------- |
| `MAX_CONNECTIONS` | 50      | Beyond this, you likely need a pool         |
| `RESERVE_PERCENT` | 20%     | Room for superuser, replication, monitoring |
| `TIMEOUT_SECS`    | 30      | Balances responsiveness vs network jitter   |
```

### 10. Failure Modes (if applicable)

Show what breaks and how to recover:

```markdown
| Failure              | Recovery                             |
| -------------------- | ------------------------------------ |
| Process crash mid-op | Reconciliation job cleans up orphans |
| Network partition    | Circuit breaker opens, fails fast    |
| Database unavailable | Retry with exponential backoff       |
```

## Writing Principles

### Use Tables Over Prose

```markdown
BAD:
"The API service uses a weight of 1.0, workers use 0.7, webhooks use 0.5."

GOOD:

| Service  | Weight | Rationale                           |
| -------- | ------ | ----------------------------------- |
| API      | 1.0    | High concurrency, short queries     |
| Worker   | 0.7    | Batch-oriented, longer transactions |
| Webhooks | 0.5    | Bursty but low sustained volume     |
```

### Show the "Why", Not Just the "What"

```markdown
BAD:
"We use S3-FIFO for cache eviction."

GOOD:
"We use S3-FIFO for cache eviction. CDN traffic is highly skewed—most objects
are accessed once. S3-FIFO's quick demotion of one-hit-wonders matches this
pattern better than TinyLFU's 1% admission window."
```

### Include Performance Numbers When Relevant

```markdown
| Operation         | Latency | Notes          |
| ----------------- | ------- | -------------- |
| Topic lookup      | 2.3 ns  | atomic.Load    |
| Fanout (500 subs) | 2.6 ms  | 192K msgs/sec  |
| Cache hit         | ~50 ns  | SIEVE eviction |
```

### Link to Specific Code

Reference exact files and functions, not vague descriptions:

```markdown
The saga pattern is implemented in `service.go:CreateVM()` with compensation
logic in `service.go:compensateCreate()`.
```

## Anti-Patterns to Avoid

1. **Restating code in English** - "The CreateVM function takes a CreateVMRequest" adds nothing
2. **Missing the "why"** - Every non-obvious decision needs explanation
3. **No diagrams** - A single ASCII diagram beats paragraphs of prose
4. **Unstable details** - Document interfaces, not implementation details that change weekly
5. **No terminology definitions** - Ambiguous terms cause bugs

## Process

When writing or improving an architecture doc:

1. **Read the code first** - Understand the actual system before documenting
2. **Identify the core mechanism** - What's the main thing this does?
3. **Find the state machine** - Most systems have one, even if implicit
4. **Ask "why" for each design choice** - Document the reasoning
5. **Draw the data flow** - ASCII art is fine
6. **Define ambiguous terms** - Especially with "NOT" column
7. **Map files to responsibilities** - Help navigation
8. **Document configuration** - Defaults and rationale

## Quality Checklist

Before finishing, verify:

- [ ] Can someone understand the system in 60 seconds from the overview?
- [ ] Is there at least one diagram?
- [ ] Are domain terms defined with a "NOT" column?
- [ ] Is there a "Design Decisions" or "Why" section?
- [ ] Does it link to specific files?
- [ ] Are configuration options documented with defaults?
- [ ] If there's a state machine, is it diagrammed with a transition table?
- [ ] If there's a trust/security model, does it explicitly state what's NOT verified?

## Reference Examples

Before writing, study these exemplary architecture docs in this codebase:

| File                                | Rating | Why It's Excellent                                                                                                                                                     |
| ----------------------------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `boxes/box-manager/ARCHITECTURE.md` | 10/10  | Comprehensive coverage: typestates, state machines, storage classes, networking, crash recovery, NATS messaging, distributed tracing. Every design decision explained. |
| `edge/ARCHITECTURE.md`              | 10/10  | Exhaustive reverse proxy docs: RFC 9111 compliance tables, S3-FIFO eviction rationale, WAF prefilter SIMD details, store abstraction layer.                            |
| `tunnel/ARCHITECTURE.md`            | 10/10  | SSH/TLS proxy with explicit trust model (what we verify AND what we don't). Circuit breakers, rate limiting, connection draining.                                      |
| `api/app/billing/ARCHITECTURE.md`   | 9/10   | Trust-based tiering, fraud scoring with signal weights, state machines. Excellent "Design Decisions" section.                                                          |
| `api/app/authz/ARCHITECTURE.md`     | 9/10   | Zanzibar-style ReBAC with "Why ReBAC over RBAC/ABAC?" section. Check algorithm, caching strategy, performance characteristics.                                         |

**Read these files** to understand the quality bar. Note how they:

- Lead with diagrams, not prose
- Define terms with "NOT" columns
- Explain "why" for every non-obvious choice
- Include state machine diagrams AND transition tables
- Document trust models explicitly

## Output Format

When creating an architecture doc, output a complete ARCHITECTURE.md file with all applicable sections. Use the exact markdown formatting shown above.

When improving an existing doc, identify which sections are missing or weak and suggest specific additions.
