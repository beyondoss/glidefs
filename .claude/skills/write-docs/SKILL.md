---
name: write-docs
description: Write documentation in the Paraglide voice. Use when writing API docs, guides, tutorials, or reference material.
allowed-tools: Read, Glob, Grep, Edit, Write
model: claude-sonnet-4-6
---

## Before Writing

**Load the verbal identity:**

```
Read: .claude/skills/brand/verbal-identity.md
```

## Docs Voice: Most Precise

Documentation is where precision matters most. Clear, direct, technical. Assume competence. Code over prose. No preamble, no context-setting.

### Key Principles

1. **Assume competence** — Don't explain what developers already know
2. **Code over prose** — Show, don't tell
3. **No preamble** — Get to the point immediately
4. **Technical words OK** — fork, snapshot, checkpoint, restore, CoW are fine here

### Example

```
POST /boxes/{id}/promote deploys a checkpoint to production.
```

Not:

```
In order to deploy your changes to production, you'll need to use our promote endpoint, which takes your checkpoint and makes it live...
```

## Structure

1. **What it does** — One sentence
2. **How to use it** — Code example
3. **Parameters/Options** — Table or list
4. **Edge cases** — Only if non-obvious

## Technical Vocabulary (OK in docs)

These need to be explained once when introduced, then used freely:

- fork
- snapshot
- checkpoint
- restore
- promote
- CoW (copy-on-write)

## Voice Calibration

| Aspect      | Docs Approach         |
| ----------- | --------------------- |
| Tone        | Neutral, precise      |
| Assumptions | Reader is competent   |
| Length      | Minimal               |
| Code:Prose  | More code, less prose |
| Warmth      | None needed           |

## Before Publishing

1. Can this be shorter?
2. Is there a code example?
3. Am I explaining something obvious?
4. Would I be annoyed reading this as a developer?
5. Does the first sentence say what this does?
