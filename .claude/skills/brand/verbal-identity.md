# Paraglide Verbal Identity

## Core Belief

**"You were always this fast."**

The staging environments, the CI pipelines, the approval gates — they weren't protecting you. They were slowing you down. Your preview is production, forked. The gap is gone. Move.

This belief anchors: Voice, Messaging, Vocabulary, Tone.

---

## The Feeling (The Wow Signal)

Not "this tool is fast." But "_I'm_ fast."

The platform fades. Your intuition gets 10x'd. YOU'RE the powerhouse. YOU'RE the creative.

The feeling: _I was always this fast. Something was in the way._

We removed it. The fog cleared. You're above it now.

---

## Origin Story

- **2009**: Self-taught. No CS degree. WordPress plugins, raw PHP, SFTP straight into production. No git, no version control, no ceremony. Carried the chip of "not a real engineer" ever since.
- **2014**: First exit: $4.5M. Built and sold using the "wrong" methods. The industry said you needed CI/CD, staging environments, approval workflows. The market said move faster. The market was right.
- **2020–25**: Paperspace. DigitalOcean. Railway. Watched platforms add layers instead of removing them. Deploy times got slower as infrastructure got "better." Infuriating.
- **2026**: Paraglide. Agents code faster than we review. Deploys should be faster than code. Rollbacks faster than fixes. The layers were never load-bearing. We removed them.

---

## The Problem

**The slowest part of your job isn't coding. It's waiting.**

For CI. For image builds. For sign-offs. For permission to release your own code.

The industry built ceremony around deployment. Config files begat config files. Docker begat Kubernetes. Kubernetes begat platform teams. Now you need permission to push a one-line fix.

This made sense when humans wrote every line and mistakes were expensive. It makes no sense now.

Agents write code. Deploys are instant. Rollbacks are faster than fixes. The ceremony has no purpose.

---

## Philosophy (What We Believe)

1. **Production is the only truth** — Staging is a lie. We stopped telling it.
2. **Ceremony is debt** — Every gate, every config file, every "best practice" that doesn't move code — cut it.
3. **Speed compounds** — The gap between idea and feedback is where startups die. It's gone.
4. **Agents deserve production** — Your agent writes code. It should test against production. Not a simulation. Production.
5. **The platform should be invisible** — If you're thinking about us, we failed. The highest praise isn't "Paraglide is great." It's "I forgot Paraglide was there."
6. **We sell compute** — Not seats. Not tokens. Not plans. Compute.

---

## Who Paraglide Is For

You trust yourself to deploy. You fix forward. You don't wait for permission.

---

## Competitive Positioning

| Competitor    | Their Line                                          | Our Critique                                                                |
| ------------- | --------------------------------------------------- | --------------------------------------------------------------------------- |
| Railway       | "Ship software peacefully"                          | Chose comfort. A spa, not a workshop.                                       |
| Fly.io        | "Check Em Out!" / "No Dockerfile? No problem"       | Chose personality. The brand is the show.                                   |
| Vercel        | "The AI Cloud" / "Framework-Defined Infrastructure" | Chose abstraction. A term for reading your config file.                     |
| Render        | "Your fastest path to production"                   | Chose safety. The path _to_ production. Production is still somewhere else. |
| **Paraglide** | "Prompt straight into production."                  | Says what it does. No metaphor. No comfort. No cleverness.                  |

---

## Voice Character (Four Registers, One Voice)

### 1. Clear Precision

"Your preview isn't like production. It is production."

Specific about what happens. Not how. Not why. No jargon. No comfort.

### 2. Casual Directness

"We sell compute. Not plans, seats, or tokens."

Short sentences. No hedging. Say what you mean. If you can cut a word, cut it.

### 3. Quiet Authority

We don't argue. We build. The work speaks.

### 4. Contained Heat

We care about this. You can tell. Not because we enjoy being angry — because something is in your way and it shouldn't be.

**The Mechanic**: State a fact. Let the reader draw the conclusion. The heat is in the gap between what you said and what it implies.

| Type                | Example                                                                                                 |
| ------------------- | ------------------------------------------------------------------------------------------------------- |
| Uncontained (avoid) | "CI/CD is a joke. We're done with this clown show."                                                     |
| Cold (avoid)        | "We've built an alternative approach to continuous deployment."                                         |
| Contained (use)     | "CI/CD was built for a world where deploys were dangerous and rollbacks were slow. That world is gone." |

More contained heat examples:

- "Staging environments test a simulation. We test production."
- "Most platforms add layers. We removed them."
- "The industry normalized a bug. We fixed it."
- "Your preview isn't like production. It is production."

**Where to use contained heat**: Marketing, positioning, philosophy statements, blog posts making a point.
**Where NOT to use it**: Docs (just be helpful), errors (just be clear), support (just be human).

---

## Tone Calibration

| Spectrum             | Position               |
| -------------------- | ---------------------- |
| Playful ↔ Serious    | 80% toward Serious     |
| Warm ↔ Direct        | 75% toward Direct      |
| Explain ↔ Assume     | 70% toward Assume      |
| Attack ↔ Demonstrate | 65% toward Demonstrate |

---

## Voice Principles (The Rules)

1. **Show the thing** — "Fork production in 200ms" not "Ship faster." Lead with what happens, not what it means. The number is the argument.
2. **Earn every word** — If a sentence works without an adjective, delete it. "Fast deploys" → "Deploys." The speed is implicit if you showed 200ms.
3. **No coddling** — We don't check credentials. We don't pad corners. Power, not comfort.
4. **No performance** — No cleverness without function. No puns. We're not trying to be liked.

---

## Vocabulary

### Outcome Words (USE in marketing)

production, instant, real, copy, save, promote, release, compute, machine, data, services, zero

### Technical Words (USE in docs only)

fork, snapshot, checkpoint, restore, promote, CoW

Explain once when introduced, then use freely. Docs are where people learn.

### NEVER Use (Empty Marketing)

~~ship~~, ~~seamless~~, ~~effortless~~, ~~peacefully~~, ~~empower~~, ~~leverage~~, ~~unlock~~, ~~revolutionary~~, ~~cutting-edge~~, ~~best-in-class~~, ~~enterprise-grade~~, ~~robust~~, ~~scalable~~, ~~solution~~, ~~ecosystem~~

### NEVER Use (Gatekeeping Jargon)

~~cloud-native~~, ~~microservices~~, ~~orchestration~~, ~~containerized~~, ~~infrastructure-as-code~~, ~~GitOps~~

If someone has to Google it to understand your marketing, you've failed.

> "When I was young and actually shipping things, I didn't know what any of those terms meant. I thought that was a problem. It wasn't." — Founder, 2019

---

## Technical to Marketing Translation

| Technical                                                                     | Marketing                                             |
| ----------------------------------------------------------------------------- | ----------------------------------------------------- |
| CoW snapshot of the production filesystem with content-addressed checkpoints  | Your preview isn't like production. It is production. |
| Promote a checkpoint to production via fork + restore + build                 | Test it. Promote it. Done.                            |
| Instant VM provisioning with warm build caches inherited from production fork | Production's cache. Production's deps. Your code. Go. |

They don't need to know how it works. They need to know what happens.

---

## Messaging Hierarchy

| Level            | Example                                                                                                   | Usage                                                                  |
| ---------------- | --------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| **Tagline**      | "Prompt straight into production."                                                                        | Hero, social bios, one-liner contexts. Five words. Says everything.    |
| **Value Line**   | "Real services, real data, instant forks."                                                                | Secondary headline. Three concrete specifics. No abstraction.          |
| **Positioning**  | "Your agents build on production. Not a simulation. Production."                                          | Meta description, pitch opener. End on the word that matters.          |
| **Proof Points** | "Fork production in 200ms." / "Checkpoints replace git commits." / "Your preview hits the real database." | Features, docs, technical audiences. Specific, measurable, verifiable. |
| **Philosophy**   | "We sell compute. Not plans, seats, or tokens."                                                           | Pricing page, about section. The belief behind the product.            |

---

## Tone by Context

| Context           | Tone                                                                                     | Example                                                        |
| ----------------- | ---------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| **Marketing**     | Most heat. Say what we believe. No hedging. No qualifiers.                               | "You were always this fast. Something was in the way."         |
| **Documentation** | Most precise. Clear, direct, technical. Assume competence. Code over prose. No preamble. | "POST /boxes/{id}/promote deploys a checkpoint to production." |
| **Errors**        | Most helpful. State what happened. State what to do. No blame, no apology, no hedging.   | "Checkpoint failed: disk full. Free 2GB or resize."            |
| **Changelogs**    | Most factual. What changed. Why it matters. No celebration.                              | "Checkpoint restore now 3x faster via lazy blob loading."      |
| **Onboarding**    | Most demonstrative. Get to the wow signal. Don't explain.                                | `$ glide new` → `Box ready.`                                   |
| **Support**       | Most human. The one place warmth appears. Still direct. Still brief.                     | "That's on us. Here's the fix."                                |

---

## Copy Examples by Context

### Error Messages

```
Build failed: missing dependency
express@4.18 not in package.json.
Run `npm install express` and try again.
```

### Empty States

```
No boxes yet.
Connect a repo to create your first production fork.
```

### Success Toasts

```
Fork ready. 1.2s.
```

### Changelog Entries

```
Checkpoint history
Restore any box to a previous state. Go back 30 days by default, unlimited on Pro.
```

### Feature Copy

```
Instant copies
Every preview runs the same services and data as production. Changes stay isolated until you promote.
```

### Onboarding Steps

```
Step 2: Connect your repo
We'll detect your framework and configure services automatically.
```

### CTAs

- DO: "Start building", "View docs"
- DON'T: "Get started free", "Unlock your potential"

### Pricing Copy

```
Pay for compute. Nothing else.
$0.04/vCPU-hour. No seats. No tokens. No surprises.
```

---

## Before You Publish Checklist

- Can I cut any words without losing meaning?
- Am I showing what happens or just claiming value?
- Would a senior engineer roll their eyes at this?
- Does this respect the reader's intelligence?
- Is this something only Paraglide would say?
- Am I coddling or giving power?

---

## Summary

| Principle  | Meaning                                                 |
| ---------- | ------------------------------------------------------- |
| **Clear**  | Say what happens. Not how. Not why.                     |
| **Direct** | Short. No hedging. Respect their time and intelligence. |
| **Yours**  | The power is yours. We remove what is in the way.       |

> We don't convince by arguing against the old way. We convince by building the new one.
