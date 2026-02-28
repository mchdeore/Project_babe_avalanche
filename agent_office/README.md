# Agent Office

Shared workspace for AI agents and humans collaborating on this repo. Contains
four things: a **task board** (orchestration), a **dispatch log**, **active task
claims**, and **plan history**. Read this entire file at the start of every session.

---

## Quick Start (do this every time)

1. Read `task_board.md` -- check if you have an assigned task from the orchestrator.
2. Read `dispatch_log.txt` -- quick overview of all work ever done on this repo.
3. Read `active_tasks/` -- check if any agent is currently working on something
   that overlaps with what you're about to do.
4. Skim the most recent plan history `.txt` files for context on recent changes.
5. Before starting work, **claim your task** (see section 1 below).
6. When done: **release your claim**, **write a plan history entry**, and
   **append one line to `dispatch_log.txt`**.

---

## 1. Active Tasks (Coordination)

**Location:** `agent_office/active_tasks/`

This is how agents avoid stepping on each other. Before modifying files, create
a claim file so other agents know what you're touching.

### Claiming a task

Create a file in `active_tasks/` named with your agent/session ID:

```
agent_office/active_tasks/<agent-id>.md
```

Use whatever ID uniquely identifies your session (a short hash, your machine
hostname + timestamp, etc.). The file contents:

```markdown
# Active Task

agent: <your identifier or machine name>
started: <ISO timestamp UTC>
status: active

## Scope

<1-3 sentences: what are you doing?>

## Files I'm Touching

- path/to/file_a.py
- path/to/file_b.py
- config.yaml (scoring section only)

## Estimated Duration

<rough guess: "~30 minutes", "a few hours", etc.>

## Notes

<anything another agent should know to avoid conflicts>
```

### Before you start working

1. List all files in `active_tasks/`
2. Read each one
3. If another agent's claimed files overlap with yours:
   - If their `status` is `active` -- **do not modify those files**. Work on
     something else, or coordinate by adding a note to your own claim file.
   - If their file looks stale (started > 2 hours ago with no updates, or the
     agent session is clearly over) -- you may delete it and proceed.
4. Create your own claim file

### When you finish

1. Delete your claim file from `active_tasks/`
2. Write a plan history entry (see section 2)
3. Append one line to `dispatch_log.txt` (see section 3)

### Stale claims

A claim is considered stale if:
- `started` is more than 2 hours ago AND there's no recent update
- The agent session that created it is clearly terminated
- The machine that created it isn't actively connected

Any agent may delete stale claims to unblock work.

---

## 2. Plan History (Progress Logs)

**Location:** `agent_office/*.txt` (files in the root of agent_office, not in active_tasks)

After completing work, write a plan history file so future agents (on any machine)
can understand what was done, why, and how things work now.

### File naming

```
YYYY-MM-DD_short-slug.txt
```

Date the plan started, plus a kebab-case name. Examples:
- `2026-02-28_insights-ml-pipeline.txt`
- `2026-03-01_fix-polymarket-slugs.txt`
- `2026-03-05_add-nhl-player-props.txt`

### File format

```
PLAN: <Name of the plan or task>
DATE: <YYYY-MM-DD>
STATUS: <in_progress | completed>
MACHINE: <hostname or identifier, if known>

CONTEXT:
<2-5 lines explaining the situation before this work began.
 What was broken, missing, or needed? Why was this work started?>

CHANGES:

[YYYY-MM-DD HH:MM UTC] [category] One-line summary
  - Detail line 1 (what changed)
  - Detail line 2 (which files)
  - Detail line 3 (why / rationale)

[YYYY-MM-DD HH:MM UTC] [category] Another change
  - ...
```

### Rules

- **One file per plan/task** -- never append to another agent's file
- Each change entry needs enough detail for a cold-start LLM to understand
  what was done, which files were affected, and why
- Update `STATUS` to `completed` when done
- Commit the file to git so it syncs across machines

### Categories

- `[feature]` -- new functionality
- `[bugfix]` -- fixing broken behaviour
- `[config]` -- configuration changes
- `[infra]` -- dependencies, DB schema, tooling
- `[refactor]` -- restructuring without behaviour change
- `[scoring]` -- AI scoring system changes
- `[ml]` -- ML pipeline changes

---

## 3. Dispatch Log (Workforce Overview)

**Location:** `agent_office/dispatch_log.txt`

A single shared file where every agent appends one line after completing work.
This gives the user (and other agents) an at-a-glance timeline of who did what,
from which machine, and where to find the details.

### Format

```
YYYY-MM-DD | <agent> <device> | <brief summary> | <plan file or "n/a">
```

- **agent** -- the LLM tool you are: `cursor`, `codex`, `copilot`, `claude`, etc.
- **device** -- the machine you're running on: `mac`, `pc`, `laptop`, `server`, etc.
- **summary** -- one sentence, keep it short (the plan .txt has the full details)
- **plan file** -- the filename of your plan history entry, or `n/a` for trivial changes

### Rules

- Always **append** to the bottom -- never edit or delete existing lines
- One line per completed task
- Keep summaries brief -- this is the overview, not the detail

---

## Cross-Machine Collaboration

This system works across machines through git:

1. **Before starting work:** `git pull` to get latest claim files and plan history
2. **After claiming a task:** `git add agent_office/ && git commit && git push`
   so agents on other machines see your claim
3. **After finishing:** delete claim, write plan history, commit and push

If two agents on different machines need to work simultaneously:
- Each claims different files in their claim files
- Non-overlapping work can proceed in parallel
- Overlapping work should be serialized (one agent waits for the other to finish)

---

## 4. Orchestration (Task Board)

**Location:** `agent_office/task_board.md`

This project uses a **mastermind orchestration model**:

- **Orchestrator (Cursor):** Plans work, breaks it into scoped tasks, assigns
  them to workers, reviews output, and plans the next round. Only the
  orchestrator edits `task_board.md`.
- **Workers (Codex instances):** Execute assigned tasks. Each worker has a
  name (Alpha, Bravo, Charlie) and runs on a specific device.
- **User (therealmc):** Acts as the message bus — copies task briefings from
  the orchestrator to each worker device, reports completion back.

### Workflow

1. User asks the orchestrator to plan a round
2. Orchestrator writes tasks to `task_board.md` (one per worker, non-overlapping)
3. User pastes the briefing to each Codex instance on each device
4. Workers: `git pull` → read task → claim → execute → report → commit + push
5. User tells orchestrator when workers are done
6. Orchestrator reviews output, plans next round

### Rules for workers

- Read `task_board.md` to find YOUR task (look for your worker name)
- Stay within the file scope listed in your task — do not touch other files
- Do NOT edit `task_board.md` — only the orchestrator writes there
- Follow the active_tasks / plan history / dispatch_log protocol as usual
- If blocked or confused, write a note in your plan history and stop

---

## Folder Structure

```
agent_office/
  README.md                              <-- you are here
  task_board.md                          <-- orchestrator's task assignments
  dispatch_log.txt                       <-- one-line-per-task workforce overview
  active_tasks/                          <-- current work claims (ephemeral)
    <agent-id>.md                        <-- one per active agent session
  2026-02-28_insights-ml-pipeline.txt    <-- plan history (permanent)
  2026-03-01_some-other-task.txt         <-- future example
```
