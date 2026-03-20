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

1. Run `git status` to see what is staged/unstaged
2. Stage relevant files with `git add <files>` (avoid `git add .` or `git add -A`)
3. Draft the commit message following the rules above
4. Commit: `git commit -m "<message>"`
5. Confirm with `git status`
