---
name: commit
description: Prepare and commit staged changes. Ensures work is on a feature or bug branch (never main), and writes a short functional commit message with no ticket numbers, author info, or emails.
disable-model-invocation: false
---

Prepare and commit staged changes following the project's git conventions.

## Branch

Check the current branch. If on `main`, stop and create a branch first:
- For features: `feature/<short-description>`
- For bugs/defects: `bug/<short-description>`

Use `git checkout -b <branch-name>` then proceed.

## Commit message

Write a single short line (≤72 chars). No ticket number, no author, no email — just what changed and why.

Good: `quote ClickHouse identifiers in generated migration SQL`
Bad: `fix: PROJ-123 fix column quoting (John Doe <john@example.com>)`

Rules:
- Imperative mood, lowercase start
- Describe the functional change and its purpose
- No period at the end
- No emojis

## Steps

1. Run `git fetch` and check if the current branch is behind its remote. If so, run `git pull` before proceeding.
2. Run `git status` to see what is staged/unstaged
3. Stage relevant files with `git add <files>` (avoid `git add .` or `git add -A`)
4. Draft the commit message following the rules above
5. Commit: `git commit -m "<message>"`
6. Confirm with `git status`
