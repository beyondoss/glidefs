---
name: audit-safety
description: Audit Rust codebase for production safety issues. Checks unsafe blocks, panic surfaces, resource leaks, concurrency footguns, lint suppression, error swallowing, and dependency risks. Use when reviewing code for production readiness, auditing unsafe usage, or evaluating a crate's safety posture.
allowed-tools: Read, Glob, Grep, Bash, LSP
model: claude-sonnet-4-6
---

# Rust Production Audit Checklist

A comprehensive checklist for auditing Rust codebases, focused on patterns that compile cleanly but cause real production issues. Organized by severity and likelihood of occurrence.

---

## 1. Memory Safety Escape Hatches

### `unsafe` blocks

- [ ] Every `unsafe` block has a `// SAFETY:` comment explaining the invariant being upheld
- [ ] `unsafe` is not used to bypass borrow checker frustration (lifetime workarounds, self-referential structs)
- [ ] No `unsafe` in application code that could be replaced with safe abstractions
- [ ] `#[deny(unsafe_code)]` is set at the crate root for application crates
- [ ] Any `#[allow(unsafe_code)]` overrides are reviewed and justified
- [ ] FFI boundaries have wrapper types that enforce invariants on the safe side
- [ ] Raw pointer dereferences have documented aliasing and validity guarantees

### `std::mem::transmute`

- [ ] No `transmute` between types with different sizes or alignments
- [ ] No `transmute` to bypass visibility or lifetime constraints
- [ ] Prefer `from_ne_bytes`, `to_ne_bytes`, or `bytemuck` for type punning
- [ ] Every `transmute` call has a comment explaining why a safe alternative isn't possible

### `unsafe impl Send` / `unsafe impl Sync`

- [ ] Every manual `Send`/`Sync` impl has a documented proof of thread safety
- [ ] No blanket `unsafe impl Send for MyType` just to make the compiler happy
- [ ] Wrapper types around raw pointers (`*mut T`) actually enforce the concurrency guarantees they claim

### `String::from_utf8_unchecked` and similar `_unchecked` functions

- [ ] Input is provably valid (e.g., came from a `String` originally)
- [ ] Prefer the checked variant unless profiling proves it's a bottleneck
- [ ] `from_raw_parts`, `get_unchecked`, `unreachable_unchecked` — same scrutiny

---

## 2. Panics and Unwinding

### `.unwrap()` and `.expect()`

- [ ] `clippy::unwrap_used` and `clippy::expect_used` are denied in CI (not just warned)
- [ ] No `#[allow(clippy::unwrap_used)]` without a comment explaining why the value is guaranteed
- [ ] Library code never panics on invalid input — return `Result` or `Option`
- [ ] `.expect()` messages describe the invariant, not the error ("config must be loaded before server starts", not "failed to unwrap")

### `panic!()`, `todo!()`, `unimplemented!()`

- [ ] No `todo!()` or `unimplemented!()` in code paths reachable in production
- [ ] `panic!()` is reserved for genuine invariant violations, not input validation
- [ ] Grep the codebase: `rg 'todo!\(|unimplemented!\(|panic!\(' --type rust`

### Array/slice indexing

- [ ] No unchecked indexing (`array[i]`) in hot paths where `i` could be out of bounds
- [ ] Prefer `.get()` and handle `None`, or prove bounds with prior checks
- [ ] Watch for off-by-one: `vec[vec.len()]` compiles, panics at runtime

### Integer overflow

- [ ] Debug builds panic on overflow (default), but release builds **wrap silently**
- [ ] Use `checked_add`, `saturating_add`, or `wrapping_add` to make overflow behavior explicit
- [ ] Audit any arithmetic on user-provided values or sizes

---

## 3. Resource Leaks

### `Box::leak()`

- [ ] Only used for truly `'static` data (singletons, global config)
- [ ] Not used as a workaround for lifetime complexity
- [ ] Grep: `rg 'Box::leak' --type rust` — every hit should be justified

### `std::mem::forget()`

- [ ] Destructors / `Drop` impls that are skipped don't hold resources (file handles, locks, network connections)
- [ ] Not used to prevent double-free — that indicates a deeper ownership problem
- [ ] `ManuallyDrop` is preferred when drop timing needs to be controlled

### `Rc` / `Arc` reference cycles

- [ ] Any graph or tree structure using `Rc`/`Arc` has `Weak` references for back-edges
- [ ] Parent-child relationships: parent owns child via `Rc`, child references parent via `Weak`
- [ ] No `Rc<RefCell<T>>` cycles — these will never be freed

### Unbounded collections

- [ ] `HashMap`, `Vec`, `VecDeque` that grow based on external input have a capacity bound or eviction policy
- [ ] Caches have a max size (LRU, TTL, or hard cap)
- [ ] Unbounded `mpsc::channel()` — prefer `sync_channel` with a bound, or document why unbounded is safe
- [ ] Log buffers, event queues, retry queues — anything that accumulates has a limit

### File descriptors and handles

- [ ] `File`, `TcpStream`, `UdpSocket` are dropped or explicitly closed
- [ ] No long-lived ownership of file handles that should be short-lived
- [ ] In `async` code: watch for held-open connections across `.await` points

---

## 4. Concurrency

### Deadlocks

- [ ] Multiple `Mutex` locks are always acquired in a consistent global order
- [ ] No lock held across `.await` (use `tokio::sync::Mutex` if needed, but prefer restructuring)
- [ ] `RwLock` write starvation: check that read-heavy workloads don't starve writers indefinitely
- [ ] No recursive locking (Rust's `Mutex` is not reentrant — will deadlock)

### Async pitfalls

- [ ] No blocking calls (`std::fs`, `std::net`, `thread::sleep`) inside `async` functions — use async equivalents
- [ ] `tokio::spawn` tasks have proper cancellation handling (check `select!` branches)
- [ ] `JoinHandle`s are not silently dropped — panics in spawned tasks are lost
- [ ] Dropping a `JoinHandle` detaches the task, it doesn't cancel it
- [ ] Watch for holding `MutexGuard` across `.await` — this is a deadlock or send error waiting to happen

### Atomics

- [ ] `Ordering::Relaxed` is only used when ordering genuinely doesn't matter
- [ ] No `Ordering::Relaxed` on flags used for synchronization (use `Acquire`/`Release` or `SeqCst`)
- [ ] Prefer higher-level primitives (`Mutex`, channels) unless profiling demands atomics

---

## 5. Error Handling

### Silent error swallowing

- [ ] No bare `let _ = fallible_call();` without a comment
- [ ] `#[deny(unused_must_use)]` is set — `Result` values cannot be silently dropped
- [ ] No `#[allow(unused_must_use)]` without justification
- [ ] `.ok()` on a `Result` — confirm the error case is truly irrelevant
- [ ] `catch_unwind` — panics are being caught, not silenced. Logged? Propagated?
- [ ] Batch operations (loops over files, retries, fan-outs) track and propagate the **worst** outcome, not just the last `Result` — a single failure mid-loop shouldn't report success

### Error type design

- [ ] Errors carry enough context to debug without reproducing (`thiserror` or manual `Display`)
- [ ] No `Box<dyn Error>` in library APIs — use concrete types
- [ ] `anyhow` is fine for applications, not for libraries
- [ ] Error chains are preserved — `.context()` or equivalent, not discarded

---

## 6. Lint Suppression and Compiler Bypasses

### `#[allow(...)]` audit

- [ ] Grep: `rg '#\[allow' --type rust` — review every suppression
- [ ] No crate-level `#![allow(warnings)]` or `#![allow(clippy::all)]`
- [ ] Each `#[allow]` has an accompanying comment with justification
- [ ] `#[allow(dead_code)]` — is this actually dead code that should be removed?
- [ ] `#[allow(clippy::unwrap_used)]` — is the unwrap actually infallible here?

### CI enforcement

- [ ] Clippy runs with `-D warnings` (deny, not warn)
- [ ] `#[deny(unsafe_code)]` at crate root for application crates
- [ ] `#[deny(unused_must_use)]` at crate root
- [ ] Clippy lints cannot be overridden by local `#[allow]` without review (enforce via CI grep or policy)

---

## 7. Process and System

### `std::process::abort()` and `std::process::exit()`

- [ ] Neither is called in library code
- [ ] Both bypass all destructors — confirm no cleanup is needed
- [ ] Exit codes are meaningful and documented

### Signal handling

- [ ] `SIGTERM` / `SIGINT` handlers exist for long-running services
- [ ] Handlers use async-signal-safe operations only (no allocations, no locks)
- [ ] Graceful shutdown drains in-flight work before exiting

---

## 8. Serialization and Data Integrity

### `serde` pitfalls

- [ ] `#[serde(deny_unknown_fields)]` on types where unknown fields indicate a problem
- [ ] `#[serde(default)]` values are correct — not just `Default::default()` when zero/empty is wrong
- [ ] Deserialization of untrusted input has size limits (prevent billion-laughs style attacks)
- [ ] Enum deserialization is forward-compatible — `#[serde(other)]` on catch-all variant if needed
- [ ] No `#[serde(skip)]` on fields that are security-relevant

### Type confusion

- [ ] Newtype wrappers for domain types that shouldn't be interchangeable (`UserId(u64)` vs `OrderId(u64)`)
- [ ] No bare `String` for structured data (URLs, paths, emails) — use validated newtypes

---

## 9. Performance Footguns

### Cloning

- [ ] No `.clone()` in hot loops without profiling justification
- [ ] `Arc::clone()` is cheap, `Vec::clone()` is not — know which you're calling
- [ ] Prefer borrowing over cloning unless ownership is required

### Allocation

- [ ] `String` and `Vec` in hot paths use `with_capacity` when size is known
- [ ] No repeated `format!()` or string concatenation in tight loops
- [ ] `collect::<Vec<_>>()` on large iterators — consider streaming/lazy alternatives
- [ ] Watch for implicit allocations: `to_string()`, `to_vec()`, `into()` on borrowed data

### Regex

- [ ] `Regex::new()` is called once and reused (not compiled per invocation)
- [ ] Use `lazy_static!` or `once_cell::Lazy` for static regex patterns
- [ ] Untrusted regex patterns have complexity limits (`regex` crate is safe, but slow patterns exist)

---

## 10. Filesystem and System Boundary Safety

These bugs live at syscall and OS boundaries — Rust's type system cannot prevent them.

### TOCTOU (Time-of-Check-Time-of-Use)

- [ ] No check-then-act on filesystem paths (e.g., `metadata()` then `open()`, `remove_file()` then `File::create()`)
- [ ] Use `OpenOptions::create_new(true)` to atomically create-or-fail without a race window
- [ ] Prefer file-descriptor-anchored operations over re-resolved path strings where possible
- [ ] Grep: `rg 'fs::remove_file|fs::rename|fs::metadata' --type rust` — look for a paired `create`/`open` call nearby

### Insecure Permission Windows

- [ ] Files and directories are not created with default permissions and then hardened via `set_permissions()` afterward — there is a window between creation and hardening
- [ ] Use `OpenOptions::mode()` (Unix) or `DirBuilder::mode()` to set permissions atomically at creation time
- [ ] Grep: `rg 'set_permissions' --type rust` — check for a preceding `create` or `create_dir` on the same path

### Path Identity vs. String Equality

- [ ] Paths guarding security decisions are not compared as raw strings — `/../`, `/./`, symlinks, and relative forms can alias the same inode while comparing unequal
- [ ] Use `fs::canonicalize()` before comparing paths used for access control
- [ ] Grep: `rg 'path ==' --type rust` — flag any equality check on `Path`/`PathBuf`/`str` used for authorization

### Trust Boundary Code Loading

- [ ] No library calls that load dynamic modules (NSS lookups, PAM, `dlopen`) _after_ crossing a privilege or chroot boundary — an attacker controls the chroot tree and can supply malicious modules
- [ ] Resolve usernames, group memberships, and hostnames _before_ calling `chroot()`, dropping privileges, or entering a sandbox
- [ ] Grep: `rg 'chroot|setuid|setgid' --type rust` — verify all NSS/resolver calls (`get_user_by_name`, `getaddrinfo`, etc.) precede the boundary crossing

---

## 11. Binary Data and Encoding Assumptions

- [ ] Stream data, file content, and filenames are handled as `&[u8]` / `Vec<u8>` / `OsStr` — not converted to `String` unless the encoding is guaranteed by the source
- [ ] No `String::from_utf8_lossy()` on data that must round-trip without corruption — it silently replaces invalid bytes with `U+FFFD`, causing data loss
- [ ] `from_utf8()` failures on filenames or path arguments are surfaced as errors, not silently skipped or unwrapped
- [ ] Binary output uses `Write::write_all()` rather than `print!` / `println!`, which assume UTF-8 and may panic or corrupt on non-UTF-8 bytes
- [ ] Grep: `rg 'from_utf8_lossy|String::from_utf8\b' --type rust` — verify each call site is intentional and won't corrupt data

---

## Quick Grep Commands

```bash
# Unsafe audit
rg 'unsafe' --type rust
cargo geiger

# Panic surface
rg '\.unwrap\(\)|\.expect\(|panic!\(|todo!\(|unimplemented!\(' --type rust

# Leak surface
rg 'Box::leak|mem::forget|ManuallyDrop' --type rust

# Lint suppression
rg '#\[allow' --type rust

# Error swallowing
rg 'let _ =|\.ok\(\);' --type rust

# Unbounded collections/channels
rg 'mpsc::channel\(\)' --type rust

# Process termination
rg 'process::abort|process::exit' --type rust

# Unchecked operations
rg '_unchecked\(' --type rust

# TOCTOU / filesystem races
rg 'fs::remove_file|fs::rename|fs::metadata' --type rust

# Insecure permission windows
rg 'set_permissions' --type rust

# Path string equality (potential identity confusion)
rg 'path ==' --type rust

# Trust boundary crossings
rg 'chroot|setuid|setgid' --type rust

# UTF-8 / binary data assumptions
rg 'from_utf8_lossy|String::from_utf8\b' --type rust
```

## Output Format

````
## Testing Audit: {target directory}

**Persona**: {persona}
**Scope**: {what was examined}

---

## Overall: {X}/10

{2-3 sentence summary. "Has solid unit tests but integration tests are thin and idempotency is untested."}

## Ratings

Use the merit sections above eg "Performance Footguns" and apply an individual rating to each with any notes.

Skip categories that don't apply.

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

| # | Gap           | Type          | Effort   | ICE   |
| - | ------------- | ------------- | -------- | ----- |
| 1 | {description} | {performance} | {effort} | {n.n} |

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
