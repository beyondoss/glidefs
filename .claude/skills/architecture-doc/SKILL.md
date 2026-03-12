---
name: architecture-doc
description: Write or improve ARCHITECTURE.md files for packages and systems. Use when creating documentation for a new package, reviewing existing architecture docs, or when asked to document how a system works.
allowed-tools: Read, Glob, Grep, Write, Edit
model: claude-sonnet-4-6
---

# Writing Excellent Architecture Documentation

**The purpose of a system is what it does** (POSIWID). Not what it was designed to do. Not what the comments say. What it *actually does* when you run it — its inputs, outputs, side effects, and failure behaviors.

Architecture documentation must be grounded in observable behavior. Read the code. Run it. Trace the data. Then write down what you found. If the doc describes intent that diverges from reality, the doc is wrong — not the code.

You are writing an ARCHITECTURE.md that will help engineers understand a system in 60 seconds, navigate the code in 5 minutes, and ship changes confidently in 30 minutes.

If engineers need to reverse-engineer the code to understand it, the documentation failed.

## Process

When writing or improving an architecture doc:

1. **Read the code** — Trace actual execution paths, not just type signatures. Follow the data from entry point to exit.
2. **Observe what the system produces** — What are the actual outputs, writes, side effects? What state does it mutate?
3. **Find the real data flow** — Not the intended one. The actual one, including error paths and edge cases that matter.
4. **Identify the state machine** — Most systems have one, even if implicit. Document the states that *exist*, not just the ones that were planned.
5. **Explain behavior that would surprise a reader** — If the code does something non-obvious, that's what needs documenting most.
6. **Record why the system behaves this way** — Design decisions explain observed behavior. "We do X because Y" is more useful than "We chose X over Y."
7. **Define terms by what they control** — A term's meaning is its operational effect, not its dictionary definition.
8. **Map files to what they actually do** — Not what their names suggest.

## Required Sections

Every architecture doc MUST have these sections in this order:

### 1. What This System Does

Start with observable behavior: what goes in, what comes out, what changes.

```markdown
# {Package} Architecture

{One sentence: what this system takes as input and what it produces as output.}
```

The summary should be falsifiable. Someone should be able to verify it by running the system.

```markdown
BAD:  "Manages VM lifecycle operations."
GOOD: "Takes VM creation requests over gRPC, provisions QEMU processes with
       virtio devices, and exposes a state machine that tracks each VM from
       pending through running to terminated."
```

### 2. Data Flow Diagram

Show what actually happens when data moves through the system. Include the error/rejection paths — they're part of what the system does.

```
Request ──► Auth ──► Authz ──► Rate Limit ──► Router ──► Backend
              │         │           │                        │
            401       403         429                      5xx
              │         │           │                        │
              └─────────┴───────────┴────────────────────────┘
                              ▼
                        Error Response
```

For complex systems, show multiple flows:

- Happy path (request/response)
- Error/rejection paths
- Background/async flows (these are often where the interesting behavior lives)
- State transitions

### 3. Concepts & Terminology

Define terms by their operational meaning — what they control, gate, or affect. Include what they are NOT to prevent misuse.

```markdown
| Term      | What It Controls                     | NOT                              |
| --------- | ------------------------------------ | -------------------------------- |
| Namespace | Which backend pool receives traffic  | Not a security/tenant boundary   |
| Node      | Which machine a VM is scheduled onto | Not the proxy or control plane   |
| Address   | The socket (IP:port) for connections | Not a DNS name or virtual IP     |
```

### 4. Core Mechanism

Explain what the system actually does with enough detail that someone could predict its behavior for a new input. This is the heart of the document.

Include:

- The algorithm or pattern, described in terms of inputs → outputs
- Key data structures and what invariants they maintain
- Invariants the system *actually enforces* (not aspirational ones)
- Code references for complex logic

```markdown
BAD:  "Uses consistent hashing for load balancing."
GOOD: "Routes requests by hashing the namespace header (xxh3) into a 64-bit
       ring with 256 virtual nodes per backend. When a backend is removed,
       only its slice of the ring redistributes — about 1/N of traffic shifts.
       See `router.go:Route()` for the lookup and `ring.go:Rebalance()` for
       membership changes."
```

### 5. State Machine (if applicable)

If the system has states, document the *actual* states and transitions — including error states and edges that only fire under failure conditions.

```
pending ──Create──► creating ──Complete──► running
                        │                      │
                      Failed              StopRequested
                        │                      │
                        ▼                      ▼
                     failed ◀── StopFailed ── stopping
```

| From     | Event           | To       | Guard           | What Actually Happens                  |
| -------- | --------------- | -------- | --------------- | -------------------------------------- |
| pending  | CreateScheduled | creating | -               | QEMU process spawned, virtio attached  |
| creating | CreateCompleted | running  | -               | Health check passes, IP assigned       |
| creating | CreateFailed    | failed   | -               | QEMU process killed, resources freed   |
| creating | CreateTimeout   | failed   | elapsed > 10min | Same as CreateFailed + timeout metric  |

The "What Actually Happens" column is critical — it connects the abstract state machine to concrete system behavior.

### 6. Why It Behaves This Way

Design decisions exist to explain *observed behavior*. Frame them as "the system does X because Y" rather than "we chose X over Y."

```markdown
### Why requests are hashed by namespace, not by source IP

The system routes by namespace because:

1. **Traffic is tenant-shaped** — a single tenant's requests should hit the same
   backend pool for connection reuse and cache locality
2. **Source IPs are unstable** — clients behind NAT/proxies share IPs, so
   IP-based hashing creates hotspots (we measured 40:1 skew in production)
3. **Namespace is always present** — it's extracted from the Host header in
   middleware, so the router never needs a fallback path
```

Note: this explains why the system *behaves the way it does*, not just why someone made a choice in the past.

### 7. Trust Boundaries (if applicable)

Document what the system *actually checks* and what it lets through unchecked. Be honest — undocumented trust assumptions are where security bugs live.

```markdown
## Trust Boundaries

**What the system verifies (rejects if invalid):**

- Certificate signed by trusted CA
- Certificate not expired
- Requested resource is in valid_principals

**What passes through unchecked:**

- Client identity beyond certificate (any valid cert is trusted)
- Authorization to access specific data (delegated to application layer)
- Request body content (no schema validation at this layer)

**Why these boundaries are where they are:**

- Application-layer auth (database password) is the real gate
- Transport security is a separate concern from authz
- Schema validation happens in the application server, not the proxy
```

### 8. Package Structure

Map files to what they actually do — behavior, not aspirational descriptions.

```markdown
| File              | What It Does                                              |
| ----------------- | --------------------------------------------------------- |
| `state/events.go` | Defines the event types that trigger state transitions    |
| `state/status.go` | Enforces transition rules; rejects invalid state changes  |
| `repo.go`         | Reads/writes VM records with row-level locking            |
| `service.go`      | Orchestrates create/delete with compensating rollbacks    |
```

### 9. Configuration

Document what each setting actually controls at runtime, not just what it's "for."

```markdown
| Variable          | Default | What It Controls                                       |
| ----------------- | ------- | ------------------------------------------------------ |
| `MAX_CONNECTIONS` | 50      | Hard cap on open DB connections; excess requests queue  |
| `RESERVE_PERCENT` | 20%     | Connections held for superuser/replication/monitoring   |
| `TIMEOUT_SECS`    | 30      | After 30s, in-flight requests get context cancellation  |
```

### 10. Failure Modes

Document what actually happens when things break — not what "should" happen.

```markdown
| Failure              | What Actually Happens                        | Recovery                             |
| -------------------- | -------------------------------------------- | ------------------------------------ |
| Process crash mid-op | Incomplete records left in DB, no cleanup    | Reconciliation job runs every 60s    |
| Network partition    | Circuit breaker opens after 5 failures       | Auto-closes after 30s half-open test |
| Database unavailable | All requests fail with 503, no queuing       | Retry with exponential backoff       |
```

## Writing Principles

### Describe Behavior, Then Explain It

The order matters: observable behavior first, rationale second. Readers need to know *what* the system does before they can evaluate *why*.

```markdown
BAD:
"We use S3-FIFO for cache eviction."

GOOD:
"The cache evicts entries using S3-FIFO: new entries start in a small probation
queue and promote to main only on second access. One-hit entries are evicted
quickly without polluting the main cache. This matches our CDN traffic pattern
where most objects are accessed exactly once."
```

### Use Tables Over Prose

```markdown
BAD:
"The API service uses a weight of 1.0, workers use 0.7, webhooks use 0.5."

GOOD:

| Service  | Weight | Observable Effect                                |
| -------- | ------ | ------------------------------------------------ |
| API      | 1.0    | Gets ~50% of connections under contention        |
| Worker   | 0.7    | Gets ~35%; batch queries tolerate higher latency |
| Webhooks | 0.5    | Gets ~15%; bursty but sheds load first           |
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

1. **Documenting intent instead of behavior** — "This module handles X" vs "This module takes Y as input and produces Z." The first is a label; the second is testable.
2. **Restating code in English** — "The CreateVM function takes a CreateVMRequest" adds nothing. Describe what the function *does to the system*.
3. **Aspirational invariants** — Don't document invariants the code doesn't enforce. If there's no check, there's no invariant.
4. **Missing error paths** — The error paths are part of what the system does. A system that returns 500 on malformed input *does* that. Document it.
5. **No diagrams** — A single ASCII diagram beats paragraphs of prose.
6. **Unstable details** — Document behavior and interfaces, not implementation details that change weekly.
7. **No terminology definitions** — Ambiguous terms cause bugs.

## Quality Checklist

Before finishing, verify:

- [ ] Does the summary describe observable input→output behavior, not just purpose?
- [ ] Could someone predict the system's response to a new input from this doc?
- [ ] Is there at least one diagram showing actual data flow (including error paths)?
- [ ] Are terms defined by what they control/gate, with a "NOT" column?
- [ ] Does the "why" section explain observed behavior, not just historical choices?
- [ ] Does it link to specific files and functions?
- [ ] Are configuration options documented by their runtime effect?
- [ ] If there's a state machine, does the transition table include "What Actually Happens"?
- [ ] If there's a trust model, does it honestly state what passes through unchecked?
- [ ] Are failure modes documented as "what actually happens," not "what should happen"?

## Output Format

When creating an architecture doc, output a complete ARCHITECTURE.md file with all applicable sections. Use the exact markdown formatting shown above.

When improving an existing doc, identify which sections are missing or weak and suggest specific additions.
