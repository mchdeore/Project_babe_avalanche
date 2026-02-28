# Task Board

**Managed by:** Cursor (orchestrator) on therealmc's main machine
**Last updated:** 2026-02-28

This file is the single source of truth for work assignments. Only the
orchestrator (Cursor) edits this file. Workers read it, execute their task,
and report back through the standard agent_office protocol (active_tasks
claim → work → plan history + dispatch log → delete claim).

---

## Worker Roster

| Worker   | Tool  | Device    | Status | Notes                          |
|----------|-------|-----------|--------|--------------------------------|
| Alpha    | Codex | laptop-local | idle   | Same machine as orchestrator   |
| Bravo    | Codex | home-pc   | idle   | Home desktop                   |
| Charlie  | Codex | laptop    | idle   | Laptop                         |

> **Orchestrator:** Cursor on laptop-local. Plans rounds, reviews output,
> assigns next tasks. Does not execute worker tasks.

---

## How This Works

### For the user (therealmc)

1. Ask the orchestrator (me, Cursor): **"Plan the next round"** or
   **"What should we work on?"**
2. I'll write tasks below under **Current Round**
3. Go to each device and paste the **Worker Briefing** to the Codex instance
4. Let them work. They'll commit + push when done
5. Come back here: **"Round N is done"** or **"Alpha finished, Bravo still going"**
6. I'll review their output and plan the next round

### For workers (Codex instances)

The user will paste you a briefing that tells you which worker you are and
which task to execute. Follow these steps:

1. `git pull` to get the latest code
2. Read `agent_office/README.md` for the full collaboration protocol
3. Read this file (`agent_office/task_board.md`) and find YOUR task
4. Check `agent_office/active_tasks/` for conflicts
5. Create your claim file in `active_tasks/`
6. Execute **only** your assigned task — stay within the listed file scope
7. When done:
   - Delete your claim file
   - Write a plan history `.txt` in `agent_office/`
   - Append one line to `agent_office/dispatch_log.txt`
   - `git add . && git commit -m "Worker <name>: <task summary>" && git push`

**Critical rules for workers:**
- Do NOT touch files outside your task's scope
- Do NOT modify `task_board.md` — only the orchestrator writes here
- If something is unclear, leave a note in your plan history file and stop
- If you find a bug unrelated to your task, note it in plan history but don't fix it

---

## Worker Briefings (copy-paste to each Codex)

### Alpha (laptop-local)

```
You are Worker Alpha on an orchestrated team. Your orchestrator is Cursor on this same machine.

1. Run: git pull
2. Read agent_office/README.md (collaboration protocol)
3. Read agent_office/task_board.md — find the task labeled **ALPHA** under "Current Round"
4. Execute ONLY that task. Do not touch files outside the scope listed in your task.
5. When done: delete your active_tasks claim, write a plan history .txt, append to dispatch_log.txt
6. Run: git add . && git commit -m "Worker Alpha: <brief summary>" && git push
```

### Bravo (home-pc)

```
You are Worker Bravo on an orchestrated team. Your orchestrator is Cursor on another machine.

1. Run: git pull
2. Read agent_office/README.md (collaboration protocol)
3. Read agent_office/task_board.md — find the task labeled **BRAVO** under "Current Round"
4. Execute ONLY that task. Do not touch files outside the scope listed in your task.
5. When done: delete your active_tasks claim, write a plan history .txt, append to dispatch_log.txt
6. Run: git add . && git commit -m "Worker Bravo: <brief summary>" && git push
```

### Charlie (laptop)

```
You are Worker Charlie on an orchestrated team. Your orchestrator is Cursor on another machine.

1. Run: git pull
2. Read agent_office/README.md (collaboration protocol)
3. Read agent_office/task_board.md — find the task labeled **CHARLIE** under "Current Round"
4. Execute ONLY that task. Do not touch files outside the scope listed in your task.
5. When done: delete your active_tasks claim, write a plan history .txt, append to dispatch_log.txt
6. Run: git add . && git commit -m "Worker Charlie: <brief summary>" && git push
```

---

## Project Roadmap

High-level goals the orchestrator is working toward. Updated as the project
evolves. Workers don't need to read this, but it provides context.

### Active Goals

1. **Get the data pipeline running end-to-end** — all ingestion services
   pulling live data into odds.db reliably
2. **Wire up the insights generator** — news scraping, NLP analysis (Ollama),
   event impact tracking, and ML predictions working with real data
3. **AI scoring system** — composite game scores driving smart opportunity
   detection
4. **Trading readiness** — payment methods tested and ready for live execution

### Completed Milestones

- [2026-02-28] Bootstrapped insights ML pipeline: deps, schema, feature
  engineering, scoring system, CLI commands

---

## Current Round: —

**Status:** No tasks assigned yet. Ask the orchestrator to plan the next round.

*When the orchestrator plans a round, tasks will appear here in this format:*

```markdown
## Current Round: 1

**Status:** in_progress
**Planned:** 2026-MM-DD
**Goal:** <one-line goal for this round>

---

### Task ALPHA-R1: <title>

**Worker:** Alpha (laptop-local)
**Status:** queued
**Objective:** <what to achieve>
**Scope (files you may touch):**
- path/to/file_a.py
- path/to/file_b.py

**Instructions:**
1. Step one
2. Step two
3. Step three

**Acceptance criteria:**
- [ ] Criterion one
- [ ] Criterion two

---

### Task BRAVO-R1: <title>

**Worker:** Bravo (home-pc)
**Status:** queued
...

---

### Task CHARLIE-R1: <title>

**Worker:** Charlie (laptop)
**Status:** queued
...
```

---

## Completed Rounds

*Archived here after all tasks in a round are done.*

(none yet)
