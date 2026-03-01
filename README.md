# ClickHouse Sync

`chsync` compares two ClickHouse schemas and generates a SQL migration to bring one in line with the other. Inputs can be live instances or `.sql` snapshot files.

## Motivation

Traditional migration tools (Flyway, Liquibase, migrate) assume the database is only ever changed through migrations. That assumption breaks the moment anyone runs an ad-hoc `ALTER TABLE` in production — and that happens: an analyst needs a column for a quick experiment, someone applies a hotfix directly, or the database is on fire and you patch it live.

`chsync` takes the opposite approach: look at what is actually in the database, compare it against what your code expects, and generate what needs to change. No migration history, no lock files, no drift surprises.

Common use cases:

- **Schema context for coding agents** — a version controlled `.sql` snapshot gives AI coding assistants an always-current view of your data model without any extra tooling.
- **Local database initialization** — seed a local or CI database directly from the snapshot, bypassing hundreds of incremental migrations.
- **Schema-first iteration** — make changes directly in a staging or production database, then use `chsync snapshot` to reflect them back into the committed model. Useful for analyst-owned databases or when you want to iterate quickly without going through a migration pipeline.
- **Environment parity** — use `chsync diff` to keep staging and production in sync and catch drift before it becomes a problem.

The snapshot file also doubles as always-up-to-date schema context for coding assistants — one file in the repo is all they need.

## Requirements

- [Docker](https://docs.docker.com/get-docker/) — required when using `.sql` files as input for `diff`, or when using `snapshot --verify`

## Installation

```sh
go install github.com/anytoe/chsync@latest
```

Or build from source:

```sh
git clone https://github.com/anytoe/chsync.git
cd chsync
go build -o chsync .
```

## Commands

### snapshot

Connects to a live ClickHouse instance and exports its schema as a set of ordered `CREATE DATABASE`, `CREATE TABLE`, and `CREATE FUNCTION` statements. Commit the output file to your repository.

```sh
chsync snapshot --from "clickhouse://user:pass@host:9000" --out schema.sql
```

Use `--verify` to replay the snapshot in a temporary Docker container and confirm it is valid:

```sh
chsync snapshot --from "clickhouse://user:pass@host:9000" --out schema.sql --verify
```

Flags:

| Flag | Default | Description |
|---|---|---|
| `--from` | required | Source DSN |
| `--out` | `schema.sql` | Output file |
| `--only-dbs` | | Comma-separated databases to include |
| `--skip-dbs` | | Comma-separated databases to skip |
| `--only-tables` | | Comma-separated tables to include |
| `--skip-tables` | | Comma-separated tables to skip |
| `--verify` | false | Replay schema in Docker to validate |
| `--verify-version` | `latest` | ClickHouse image version for verification |

### diff

Compares two schemas and writes a SQL migration. Sources can be `.sql` files or live DSNs. When a `.sql` file is given, `chsync` spins up a temporary Docker container to load it.

```sh
chsync diff --from current.sql --to desired.sql --out migration.sql
```

`--from` is your current state, `--to` is your desired state. The migration transforms `--from` into `--to`.

Flags:

| Flag | Default | Description |
|---|---|---|
| `--from` | required | Source: DSN or `.sql` file |
| `--to` | required | Target: DSN or `.sql` file |
| `--out` | `migration.sql` | Output file |
| `--only-dbs` | | Comma-separated databases to include |
| `--skip-dbs` | | Comma-separated databases to skip |
| `--only-tables` | | Comma-separated tables to include |
| `--skip-tables` | | Comma-separated tables to skip |

## Examples

Snapshot production, diff against the repo snapshot, review, then apply:

```sh
# Export current production schema
chsync snapshot --from "clickhouse://user:pass@prod:9000" --out prod_current.sql

# Generate migration from current → desired
chsync diff --from prod_current.sql --to schema.sql --out migration.sql

# Review migration.sql, then apply
cat migration.sql | clickhouse-client --host prod --port 9000 --multiquery
```

Diff two live instances directly:

```sh
chsync diff \
  --from "clickhouse://user:pass@staging:9000" \
  --to   "clickhouse://user:pass@prod:9000" \
  --out  migration.sql
```

Scope the diff to a single database:

```sh
chsync diff --from prod_current.sql --to schema.sql --only-dbs analytics
```

## Supported functionality

### snapshot

| Feature | Supported |
|---|---|
| Databases | yes |
| Tables (MergeTree family) | yes |
| Views | yes (exported as-is) |
| Materialized views | yes (exported as-is) |
| SQL UDFs | yes |
| Executable UDFs | no (XML-based, no create_query) |
| Dictionaries | yes (exported as-is) |

### diff / migration

| Feature | Supported |
|---|---|
| Create / drop database | yes |
| Create / drop / rename table | yes |
| Add / drop / rename column | yes |
| Modify column type or default | yes |
| Table engine change | yes (drop + recreate) |
| ORDER BY change | yes (drop + recreate) |
| Create / drop SQL UDF | yes |
| Modify SQL UDF | yes (drop + recreate) |
| Column codec (compression) | no |
| Column position | yes |
| Column comment | no |
| Column TTL | no |
| Column ALIAS / MATERIALIZED / EPHEMERAL | no |
| Table TTL | no |
| Table comment | no |
| PARTITION BY change | no |
| SAMPLE BY change | no |
| Projections | no |
| Constraints | no |
| Indexes | no |
| Table settings | no |
| Views | no |
| Materialized views | no |
| Dictionaries | no |

## Notes

This tool is not battle-tested in production. The diff engine has known gaps and edge cases. Review every generated migration before executing it. No warranty of any kind.
