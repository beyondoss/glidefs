## System Design

**IMPORTANT**: We seek the minimum effective abstraction. Elegant simplicity. Composable parts that "just work".

**Performance is a feature, not an optimization pass.**

- Do less work. The fastest code is code that doesn't run.
- Minimize allocations. Reuse where it matters.
- Parallelize only when the work itself is the bottleneck—not as a first instinct.
- Measure before you optimize, but design with performance in mind from the start.

## Operations & State

All operations that modify state—infrastructure (ZFS, OVN, OVS, iptables, TAP devices) and application—**must be idempotent and atomic**.

**Idempotent**: Running an operation multiple times produces the same result as running it once.

- Check before create; don't error if it exists
- Check before destroy; don't error if it's gone
- Safe to retry after network failures or crashes

**Atomic (or safe)**: An operation either fully succeeds, fully fails, or leaves the system in a valid intermediate state that subsequent retries can recover from.

- Multi-step operations should use transactions or compensating actions
- If you can't make it atomic, make the intermediate states safe to observe

These properties are critical for crash recovery, distributed coordination, and reasoning about system behavior.

## Testing

Our approach to testing is to create **USEFUL** tests that exercise the system's behavior under various conditions. That means integration tests that cover a wide range of scenarios, including edge cases and error conditions, rather than unit tests that focus on individual components.
