# chsync - ClickHouse Schema Diff Tool

CLI tool that compares two ClickHouse schemas and generates SQL migration statements. Inputs can be live ClickHouse instances (via DSN) or `.sql` snapshot files (loaded into temporary Docker containers).

## Commands

- `chsync snapshot --from <DSN>` - Export a ClickHouse instance schema to a `.sql` file. Optional `--verify` replays it in Docker.
- `chsync diff --from <source> --to <target>` - Compare two schemas and output a migration file. Sources can be DSNs or `.sql` files.
- `go test ./...` - Run all tests.

## Core Concepts

- `from` = current state, `to` = desired state. Migration transforms `from` into `to`.
- **Schema** - Represents one ClickHouse instance (databases, tables, columns, functions).
- **CombinedSchema** - Merged diff of two schemas. Each element tagged with Presence: Source, Target, or Both.
- **SyncPlan** - Migration plan with alternative Strategies (e.g., optimistic with renames vs conservative drop/create). Each Strategy has ordered Operations with SQL statements.

## Flow

**diff:**
1. Load both sources into `Schema` (from live instance or .sql file via Docker)
2. Merge into `CombinedSchema` — aligns databases/tables/columns, tags each with Presence
3. Generate `SyncPlan` from combined — produces Strategies with ordered SQL operations
4. Write migration SQL to output file

**snapshot:**
1. Load schema from live instance into `Schema`
2. Export as ordered SQL CREATE statements
3. Optionally verify by replaying in a Docker container
4. Write to output file

## Sync Plan Strategy

**Identity:** Two objects (databases, tables, columns) are matched by name within their parent scope. Same name = same object.

**When objects exist in both schemas:**
- Columns: type or default expression changed → `MODIFY COLUMN`
- Tables: ENGINE or ORDER BY changed → **drop + recreate** (cannot be ALTERed in ClickHouse)
- Functions: body changed → **drop + recreate**

**When objects exist in only one schema:**
- Target-only → CREATE, Source-only → DROP

**Rename detection:** When a source-only and target-only table/column pair exceeds a similarity threshold, emit RENAME instead of drop+create.
- Table similarity: Jaccard similarity of column name sets (threshold: 0.80)
- Column similarity: weighted blend of name similarity (0.7) and type match (0.3) (threshold: 0.70)
- If a rename candidate also has engine/ORDER BY changes → falls back to drop+create

## Code Structure

```
├── main.go                              CLI entry point (cobra). Commands: diff, snapshot.
└── internal/
    ├── models/
    │   ├── schema.go                    Schema, Database, Table, Column types
    │   ├── combined.go                  CombinedSchema, diff logic (NewCombinedSchema)
    │   ├── syncplan.go                  SyncPlan generator (Strategy, Operation, SQL generation)
    │   ├── statements.go               SQLStatements: parse/serialize .sql snapshot files
    │   └── schemabuilder.go            Fluent test helper for building Schema fixtures
    └── repositories/
        ├── clickhouse/
        │   ├── client.go               ClickHouse connection wrapper
        │   ├── loadschema.go           Load Schema from live instance via system tables
        │   └── export.go               Export schema as ordered SQL CREATE statements
        └── docker/
            └── manager.go              Testcontainers: spin up ClickHouse in Docker for verification
```

## Testing

Tests use `schemabuilder.go` to construct schemas: `baseSchema().addColumn(...).removeTable(...).build()`. The builder mutates Schema objects; Combined/SyncPlan handle diffing.

Run tests: `go test ./...`

## TODO

Only implement when explicitly asked.

- [ ] Handle table dependencies and ordering
- [ ] Add support for views and materialized views in diff
- [ ] Add dry-run mode
- [ ] Add apply command to execute migrations directly
- [ ] Migration CREATE TABLE statements include full column definitions
- [ ] Version handling (min supported ClickHouse version, compatibility notes)
- [ ] Add dictionaries to snapshot and diff
- [ ] ALTER [TEMPORARY] TABLE [db].name [ON CLUSTER cluster] ADD|DROP|RENAME|CLEAR|COMMENT|{MODIFY|ALTER}|MATERIALIZE COLUMN ...
- [ ] README: document that DROP COLUMN and MODIFY COLUMN (type change) are ClickHouse mutations (background, async). Apply command should poll system.mutations before executing the next statement.
- [ ] Bool/UInt8 version compatibility: ClickHouse introduced Bool in 22.4; older instances store/report Bool columns as UInt8 in system.columns.type. Treat Bool and UInt8 as equivalent in type comparison when one server predates 22.4.
