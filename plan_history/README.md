# Plan History

This folder contains one `.txt` file per plan or task that has been executed on the codebase. Its purpose is to give any agent (LLM or human) on any machine instant context on what work has been done, what changed, and why.

## How to read

Start by listing the files -- they're named `YYYY-MM-DD_short-slug.txt` and sorted chronologically. Read the most recent files first for current context, or read all of them for the full progression.

Each file has:
- **PLAN** -- name of the task
- **DATE** -- when it started
- **STATUS** -- `in_progress` or `completed`
- **CONTEXT** -- what the situation was before work began
- **CHANGES** -- timestamped entries describing each thing that was done

## How to write

When you complete a plan or task that modifies the codebase:

1. Create a new file: `YYYY-MM-DD_short-slug.txt`
2. Use the format below
3. One file per plan -- never append to another agent's file (avoids merge conflicts)
4. Each change entry should have enough detail for a cold-start LLM to understand what was done, which files were affected, and why

### Template

```
PLAN: <Name of the plan or task>
DATE: <YYYY-MM-DD>
STATUS: <in_progress | completed>

CONTEXT:
<2-5 lines explaining the situation before this work began>

CHANGES:

[YYYY-MM-DD HH:MM UTC] [category] One-line summary
  - Detail line 1 (what changed)
  - Detail line 2 (which files)
  - Detail line 3 (why / rationale)
```

### Categories

- `[feature]` -- new functionality
- `[bugfix]` -- fixing broken behaviour
- `[config]` -- configuration changes
- `[infra]` -- dependencies, DB schema, tooling
- `[refactor]` -- restructuring without behaviour change
- `[scoring]` -- AI scoring system changes
- `[ml]` -- ML pipeline changes
