package models

import (
	"reflect"
	"testing"
)

// expectedOperation describes what operation we expect to find in the generated plan
type expectedOperation struct {
	level       OperationLevel
	action      OperationAction
	canLoseData *bool    // nil means don't check, true/false means must match
	statements  []string // nil means don't check, otherwise must match exactly
}

func TestSyncPlanGenerator_ColumnPositionOperations(t *testing.T) {
	// baseSchema users table has columns: [id, name]

	tests := []struct {
		name           string
		from           func() Schema
		to             func() Schema
		wantOperations []expectedOperation
	}{
		{
			// target [email, id, name] → new column at front
			name: "new column added FIRST",
			from: func() Schema { return baseSchema().build() },
			to: func() Schema {
				return baseSchema().
					addColumn("db1", "users", "email", "String", 0).
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelColumn, action: ActionCreate, statements: []string{"ALTER TABLE `db1`.`users` ADD COLUMN `email` String FIRST;"}},
			},
		},
		{
			// target [id, email, name] → new column inserted after id
			name: "new column added AFTER a specific column",
			from: func() Schema { return baseSchema().build() },
			to: func() Schema {
				return baseSchema().
					addColumn("db1", "users", "email", "String", 1).
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelColumn, action: ActionCreate, statements: []string{"ALTER TABLE `db1`.`users` ADD COLUMN `email` String AFTER `id`;"}},
			},
		},
		{
			// source [id, name, email], target [id, email, name] → email moved forward, no other change
			name: "existing column moved only",
			from: func() Schema {
				return baseSchema().
					addColumn("db1", "users", "email", "String").
					build()
			},
			to: func() Schema {
				return baseSchema().
					addColumn("db1", "users", "email", "String", 1).
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelColumn, action: ActionAlter, statements: []string{"ALTER TABLE `db1`.`users` MODIFY COLUMN `email` String AFTER `id`;"}},
			},
		},
		{
			// Regression: source has an extra column between id and name; when it is removed
			// the target index of name shifts from 2 → 1 but name has not actually moved.
			// No MOVE operation should be emitted — only the DROP for extra.
			name: "no move when preceding source-only column is removed (idempotency)",
			from: func() Schema {
				return baseSchema().
					addColumn("db1", "users", "extra", "String", 1). // [id, extra, name]
					build()
			},
			to: func() Schema { return baseSchema().build() }, // [id, name]
			wantOperations: []expectedOperation{
				{level: LevelColumn, action: ActionDrop, statements: []string{"ALTER TABLE `db1`.`users` DROP COLUMN IF EXISTS `extra`;"}},
			},
		},
		{
			// source [id, old_name, email], target [id, email, name]
			// old_name → name: rename detected (name similarity 0.8, type match → combined 0.86 ≥ 0.70)
			// email: position moves from 2 → 1 (AFTER id)
			name: "column moved and renamed",
			from: func() Schema {
				return baseSchema().
					removeColumn("db1", "users", "name").
					addColumn("db1", "users", "old_name", "String").
					addColumn("db1", "users", "email", "String").
					build()
			},
			to: func() Schema {
				return baseSchema().
					addColumn("db1", "users", "email", "String", 1).
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelColumn, action: ActionRename, statements: []string{"ALTER TABLE `db1`.`users` RENAME COLUMN `old_name` TO `name`;"}},
				{level: LevelColumn, action: ActionAlter, statements: []string{"ALTER TABLE `db1`.`users` MODIFY COLUMN `email` String AFTER `id`;"}},
			},
		},
	}

	runTests(t, tests)
}

func TestSyncPlanGenerator_ColumnOperations(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }

	tests := []struct {
		name           string
		from           func() Schema
		to             func() Schema
		wantOperations []expectedOperation
	}{
		{
			name: "column added",
			from: func() Schema { return baseSchema().build() },
			to: func() Schema {
				return baseSchema().
					addColumn("db1", "users", "email", "String").
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelColumn, action: ActionCreate, canLoseData: boolPtr(false), statements: []string{"ALTER TABLE `db1`.`users` ADD COLUMN `email` String AFTER `name`;"}},
			},
		},
		{
			name: "column removed",
			from: func() Schema {
				return baseSchema().
					addColumn("db1", "users", "email", "String").
					build()
			},
			to: func() Schema { return baseSchema().build() },
			wantOperations: []expectedOperation{
				{level: LevelColumn, action: ActionDrop, canLoseData: boolPtr(true), statements: []string{"ALTER TABLE `db1`.`users` DROP COLUMN IF EXISTS `email`;"}},
			},
		},
		{
			name: "column type changed",
			from: func() Schema {
				return baseSchema().
					setColumnType("db1", "users", "id", "Int32").
					build()
			},
			to: func() Schema {
				return baseSchema().
					setColumnType("db1", "users", "id", "Int64").
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelColumn, action: ActionAlter, statements: []string{"ALTER TABLE `db1`.`users` MODIFY COLUMN `id` Int64;"}},
			},
		},
		{
			name: "column default added",
			from: func() Schema {
				return baseSchema().
					setColumnDefault("db1", "users", "name", "").
					build()
			},
			to: func() Schema {
				return baseSchema().
					setColumnDefault("db1", "users", "name", "'unknown'").
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelColumn, action: ActionAlter, statements: []string{"ALTER TABLE `db1`.`users` MODIFY COLUMN `name` String DEFAULT 'unknown';"}},
			},
		},
		{
			name: "column default expression changed",
			from: func() Schema {
				return baseSchema().
					addColumn("db1", "users", "end_time", "DateTime").
					setColumnDefault("db1", "users", "end_time", "addHours(start_time, 6)").
					build()
			},
			to: func() Schema {
				return baseSchema().
					addColumn("db1", "users", "end_time", "DateTime").
					setColumnDefault("db1", "users", "end_time", "addHours(end_time, 6)").
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelColumn, action: ActionAlter, statements: []string{"ALTER TABLE `db1`.`users` MODIFY COLUMN `end_time` DateTime DEFAULT addHours(end_time, 6);"}},
			},
		},
		{
			name: "column default removed",
			from: func() Schema {
				return baseSchema().
					setColumnDefault("db1", "users", "name", "'unknown'").
					build()
			},
			to: func() Schema {
				return baseSchema().
					setColumnDefault("db1", "users", "name", "").
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelColumn, action: ActionAlter, statements: []string{"ALTER TABLE `db1`.`users` MODIFY COLUMN `name` String REMOVE DEFAULT;"}},
			},
		},
		{
			name: "column rename - high similarity",
			from: func() Schema {
				return baseSchema().
					removeColumn("db1", "users", "name").
					addColumn("db1", "users", "full_name", "String").
					build()
			},
			to: func() Schema { return baseSchema().build() }, // has "name" column
			wantOperations: []expectedOperation{
				{level: LevelColumn, action: ActionRename, statements: []string{"ALTER TABLE `db1`.`users` RENAME COLUMN `full_name` TO `name`;"}},
			},
		},
		{
			name: "multiple columns added and removed",
			from: func() Schema {
				return baseSchema().
					addColumn("db1", "users", "old_field", "String").
					build()
			},
			to: func() Schema {
				return baseSchema().
					addColumn("db1", "users", "email", "String").
					addColumn("db1", "users", "phone", "String").
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelColumn, action: ActionDrop, statements: []string{"ALTER TABLE `db1`.`users` DROP COLUMN IF EXISTS `old_field`;"}},
				{level: LevelColumn, action: ActionCreate, statements: []string{"ALTER TABLE `db1`.`users` ADD COLUMN `email` String AFTER `name`;"}},
				{level: LevelColumn, action: ActionCreate, statements: []string{"ALTER TABLE `db1`.`users` ADD COLUMN `phone` String AFTER `email`;"}},
			},
		},
	}

	runTests(t, tests)
}

func TestSyncPlanGenerator_TableOperations(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }

	tests := []struct {
		name           string
		from           func() Schema
		to             func() Schema
		wantOperations []expectedOperation
	}{
		{
			name: "table created",
			from: func() Schema { return baseSchema().build() },
			to: func() Schema {
				return baseSchema().
					addTable("db1", "products", "MergeTree", []string{"id"}, []Column{
						{Name: "id", Type: "Int32"},
						{Name: "name", Type: "String"},
					}).
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelTable, action: ActionCreate, canLoseData: boolPtr(false), statements: []string{
					"CREATE TABLE `db1`.`products` (`id` Int32, `name` String) ENGINE = MergeTree ORDER BY (id);",
				}},
			},
		},
		{
			name: "table dropped",
			from: func() Schema { return baseSchema().build() },
			to: func() Schema {
				return baseSchema().
					removeTable("db1", "orders").
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelTable, action: ActionDrop, canLoseData: boolPtr(true), statements: []string{"DROP TABLE IF EXISTS `db1`.`orders`;"}},
			},
		},
		{
			name: "table engine changed",
			from: func() Schema {
				return baseSchema().
					setTableEngine("db1", "users", "MergeTree").
					build()
			},
			to: func() Schema {
				return baseSchema().
					setTableEngine("db1", "users", "ReplacingMergeTree").
					build()
			},
			wantOperations: []expectedOperation{
				// Engine change typically requires drop + create
				{level: LevelTable, action: ActionDrop, statements: []string{"DROP TABLE IF EXISTS `db1`.`users`;"}},
				{level: LevelTable, action: ActionCreate, statements: []string{
					"CREATE TABLE `db1`.`users` (`id` Int32, `name` String) ENGINE = ReplacingMergeTree ORDER BY (id);",
				}},
			},
		},
		{
			name: "table order by changed",
			from: func() Schema {
				return baseSchema().
					setTableOrderBy("db1", "users", []string{"id"}).
					build()
			},
			to: func() Schema {
				return baseSchema().
					setTableOrderBy("db1", "users", []string{"id", "name"}).
					build()
			},
			wantOperations: []expectedOperation{
				// ORDER BY change requires table recreation
				{level: LevelTable, action: ActionDrop, statements: []string{"DROP TABLE IF EXISTS `db1`.`users`;"}},
				{level: LevelTable, action: ActionCreate, statements: []string{
					"CREATE TABLE `db1`.`users` (`id` Int32, `name` String) ENGINE = MergeTree ORDER BY (id, name);",
				}},
			},
		},
		{
			name: "table primary key changed",
			from: func() Schema {
				return baseSchema().
					setTablePrimaryKey("db1", "users", []string{"id"}).
					build()
			},
			to: func() Schema { return baseSchema().build() }, // no explicit PrimaryKey
			wantOperations: []expectedOperation{
				{level: LevelTable, action: ActionDrop, canLoseData: boolPtr(true), statements: []string{"DROP TABLE IF EXISTS `db1`.`users`;"}},
				{level: LevelTable, action: ActionCreate, canLoseData: boolPtr(false), statements: []string{
					"CREATE TABLE `db1`.`users` (`id` Int32, `name` String) ENGINE = MergeTree ORDER BY (id);",
				}},
			},
		},
		{
			name: "table primary key differs from order by",
			from: func() Schema { return baseSchema().build() },
			to: func() Schema {
				return baseSchema().
					setTableOrderBy("db1", "users", []string{"id", "name"}).
					setTablePrimaryKey("db1", "users", []string{"id"}).
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelTable, action: ActionDrop, statements: []string{"DROP TABLE IF EXISTS `db1`.`users`;"}},
				{level: LevelTable, action: ActionCreate, statements: []string{
					"CREATE TABLE `db1`.`users` (`id` Int32, `name` String) ENGINE = MergeTree ORDER BY (id, name) PRIMARY KEY (id);",
				}},
			},
		},
		{
			name: "table dropped and different table created",
			from: func() Schema { return baseSchema().build() },
			to: func() Schema {
				return baseSchema().
					removeTable("db1", "orders").
					addTable("db1", "purchases", "MergeTree", []string{"id"}, []Column{
						{Name: "id", Type: "Int32"},
					}).
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelTable, action: ActionDrop, statements: []string{"DROP TABLE IF EXISTS `db1`.`orders`;"}},
				{level: LevelTable, action: ActionCreate, statements: []string{
					"CREATE TABLE `db1`.`purchases` (`id` Int32) ENGINE = MergeTree ORDER BY (id);",
				}},
			},
		},
		{
			name: "table rename - high similarity",
			from: func() Schema {
				return baseSchema().
					removeTable("db1", "users").
					addTable("db1", "user_accounts", "MergeTree", []string{"id"}, []Column{
						{Name: "id", Type: "Int32"},
						{Name: "name", Type: "String"},
					}).
					build()
			},
			to: func() Schema { return baseSchema().build() }, // has "users" table
			wantOperations: []expectedOperation{
				// With high similarity (same columns), should generate RENAME
				{level: LevelTable, action: ActionRename, statements: []string{"RENAME TABLE `db1`.`user_accounts` TO `db1`.`users`;"}},
			},
		},
		{
			name: "table rename with order by change and column removal requires drop and recreate",
			// from has "user_accounts" with 5 columns (ORDER BY id)
			// to   has "users"         with 4 columns (ORDER BY id, name) — status removed
			// column similarity = 4/5 = 80% → rename detection triggers, but ORDER BY changed
			// expected: DROP user_accounts + CREATE users (not just RENAME)
			from: func() Schema {
				return baseSchema().
					removeTable("db1", "users").
					addTable("db1", "user_accounts", "MergeTree", []string{"id"}, []Column{
						{Name: "id", Type: "Int32"},
						{Name: "name", Type: "String"},
						{Name: "email", Type: "String"},
						{Name: "phone", Type: "String"},
						{Name: "status", Type: "String"},
					}).
					build()
			},
			to: func() Schema {
				return baseSchema().
					removeTable("db1", "users").
					addTable("db1", "users", "MergeTree", []string{"id", "name"}, []Column{
						{Name: "id", Type: "Int32"},
						{Name: "name", Type: "String"},
						{Name: "email", Type: "String"},
						{Name: "phone", Type: "String"},
					}).
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelTable, action: ActionDrop, canLoseData: boolPtr(true), statements: []string{"DROP TABLE IF EXISTS `db1`.`user_accounts`;"}},
				{level: LevelTable, action: ActionCreate, canLoseData: boolPtr(false), statements: []string{
					"CREATE TABLE `db1`.`users` (`id` Int32, `name` String, `email` String, `phone` String) ENGINE = MergeTree ORDER BY (id, name);",
				}},
			},
		},
	}

	runTests(t, tests)
}

func TestSyncPlanGenerator_DatabaseOperations(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }

	tests := []struct {
		name           string
		from           func() Schema
		to             func() Schema
		wantOperations []expectedOperation
	}{
		{
			name: "database created",
			from: func() Schema { return baseSchema().build() },
			to: func() Schema {
				return baseSchema().
					addDatabase("db2").
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelDatabase, action: ActionCreate, canLoseData: boolPtr(false), statements: []string{"CREATE DATABASE `db2`;"}},
			},
		},
		{
			name: "database dropped",
			from: func() Schema {
				return baseSchema().
					addDatabase("db2").
					build()
			},
			to: func() Schema { return baseSchema().build() },
			wantOperations: []expectedOperation{
				{level: LevelDatabase, action: ActionDrop, canLoseData: boolPtr(true), statements: []string{"DROP DATABASE IF EXISTS `db2`;"}},
			},
		},
	}

	runTests(t, tests)
}

func TestSyncPlanGenerator_FunctionOperations(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }

	tests := []struct {
		name           string
		from           func() Schema
		to             func() Schema
		wantOperations []expectedOperation
	}{
		{
			name: "function added",
			from: func() Schema { return baseSchema().build() },
			to: func() Schema {
				return baseSchema().
					addFunction("double", "CREATE FUNCTION double AS (x) -> x * 2").
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelFunction, action: ActionCreate, canLoseData: boolPtr(false), statements: []string{"CREATE FUNCTION double AS (x) -> x * 2;"}},
			},
		},
		{
			name: "function removed",
			from: func() Schema {
				return baseSchema().
					addFunction("double", "CREATE FUNCTION double AS (x) -> x * 2").
					build()
			},
			to: func() Schema { return baseSchema().build() },
			wantOperations: []expectedOperation{
				{level: LevelFunction, action: ActionDrop, canLoseData: boolPtr(true), statements: []string{"DROP FUNCTION IF EXISTS double;"}},
			},
		},
		{
			name: "function body changed",
			from: func() Schema {
				return baseSchema().
					addFunction("double", "CREATE FUNCTION double AS (x) -> x * 2").
					build()
			},
			to: func() Schema {
				return baseSchema().
					addFunction("double", "CREATE FUNCTION double AS (x) -> x + x").
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelFunction, action: ActionDrop, canLoseData: boolPtr(true), statements: []string{"DROP FUNCTION IF EXISTS double;"}},
				{level: LevelFunction, action: ActionCreate, canLoseData: boolPtr(false), statements: []string{"CREATE FUNCTION double AS (x) -> x + x;"}},
			},
		},
		{
			name: "function unchanged",
			from: func() Schema {
				return baseSchema().
					addFunction("double", "CREATE FUNCTION double AS (x) -> x * 2").
					build()
			},
			to: func() Schema {
				return baseSchema().
					addFunction("double", "CREATE FUNCTION double AS (x) -> x * 2").
					build()
			},
			wantOperations: []expectedOperation{},
		},
	}

	runTests(t, tests)
}

func TestSyncPlanGenerator_MixedOperations(t *testing.T) {
	tests := []struct {
		name           string
		from           func() Schema
		to             func() Schema
		wantOperations []expectedOperation
	}{
		{
			name: "multiple changes across tables",
			from: func() Schema {
				return baseSchema().
					addColumn("db1", "users", "old_field", "String").
					build()
			},
			to: func() Schema {
				return baseSchema().
					addColumn("db1", "users", "email", "String").
					removeTable("db1", "orders").
					addTable("db1", "products", "MergeTree", []string{"id"}, []Column{
						{Name: "id", Type: "Int32"},
					}).
					build()
			},
			wantOperations: []expectedOperation{
				{level: LevelColumn, action: ActionDrop, statements: []string{"ALTER TABLE `db1`.`users` DROP COLUMN IF EXISTS `old_field`;"}},
				{level: LevelColumn, action: ActionCreate, statements: []string{"ALTER TABLE `db1`.`users` ADD COLUMN `email` String AFTER `name`;"}},
				{level: LevelTable, action: ActionDrop, statements: []string{"DROP TABLE IF EXISTS `db1`.`orders`;"}},
				{level: LevelTable, action: ActionCreate, statements: []string{
					"CREATE TABLE `db1`.`products` (`id` Int32) ENGINE = MergeTree ORDER BY (id);",
				}},
			},
		},
		{
			name:           "no changes",
			from:           func() Schema { return baseSchema().build() },
			to:             func() Schema { return baseSchema().build() },
			wantOperations: []expectedOperation{},
		},
	}

	runTests(t, tests)
}

// sampleTypeAliases returns a representative subset of the alias map that
// system.data_type_families produces. Used in unit tests instead of a live DB query.
func sampleTypeAliases() map[string]string {
	return map[string]string{
		"BOOL":              "Bool",
		"BOOLEAN":           "Bool",
		"INTEGER":           "Int32",
		"INT":               "Int32",
		"TINYINT":           "Int8",
		"SMALLINT":          "Int16",
		"BIGINT":            "Int64",
		"FLOAT":             "Float32",
		"DOUBLE":            "Float64",
		"DOUBLE PRECISION":  "Float64",
		"REAL":              "Float32",
		"VARCHAR":           "String",
		"TEXT":              "String",
		"CHAR":              "String",
		"TINYINT UNSIGNED":  "UInt8",
		"SMALLINT UNSIGNED": "UInt16",
		"INTEGER UNSIGNED":  "UInt32",
		"INT UNSIGNED":      "UInt32",
		"BIGINT UNSIGNED":   "UInt64",
		"TIMESTAMP":         "DateTime",
	}
}

// runTests is a helper that executes table-driven tests
func runTests(t *testing.T, tests []struct {
	name           string
	from           func() Schema
	to             func() Schema
	wantOperations []expectedOperation
}) {
	runTestsWithConfig(t, tests, GeneratorConfig{
		TableRenameSimilarityThreshold:  0.80,
		ColumnRenameSimilarityThreshold: 0.70,
	})
}

// runTestsWithConfig is like runTests but accepts an explicit GeneratorConfig.
func runTestsWithConfig(t *testing.T, tests []struct {
	name           string
	from           func() Schema
	to             func() Schema
	wantOperations []expectedOperation
}, config GeneratorConfig) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from := tt.from()
			to := tt.to()
			combined := NewCombinedSchema(from, to)

			generator := NewSyncPlanGenerator(config)

			plan := generator.Generate(combined)

			if len(plan.Strategies) == 0 {
				t.Fatal("Expected at least one strategy")
			}

			strategy := plan.Strategies[0]

			// Check that we have the expected number of operations
			if len(strategy.Operations) != len(tt.wantOperations) {
				t.Errorf("Expected %d operations, got %d", len(tt.wantOperations), len(strategy.Operations))
				t.Logf("Got operations:")
				for _, op := range strategy.Operations {
					t.Logf("  - %s %s (canLoseData: %v) statements: %v", op.Level, op.Action, op.CanLoseData, op.Statements)
				}
			}

			// Check that each expected operation exists
			for _, want := range tt.wantOperations {
				if !hasOperationWithDestructive(strategy, want) {
					t.Errorf("Missing expected operation: %s %s", want.level, want.action)
				}
			}
		})
	}
}

func TestSyncPlanGenerator_TypeNormalization(t *testing.T) {
	cfg := GeneratorConfig{
		TableRenameSimilarityThreshold:  0.80,
		ColumnRenameSimilarityThreshold: 0.70,
		TypeAliases:                     sampleTypeAliases(),
	}

	tests := []struct {
		name           string
		from           func() Schema
		to             func() Schema
		wantOperations []expectedOperation
	}{
		{
			// INTEGER is a SQL alias for Int32 — should not generate MODIFY COLUMN
			name: "SQL alias INTEGER equals canonical Int32",
			from: func() Schema {
				return baseSchema().setColumnType("db1", "users", "id", "INTEGER").build()
			},
			to: func() Schema { return baseSchema().build() }, // id is Int32
			wantOperations: []expectedOperation{},
		},
		{
			// Reverse: canonical on left, alias on right
			name: "canonical Int32 equals SQL alias INTEGER",
			from: func() Schema { return baseSchema().build() }, // id is Int32
			to: func() Schema {
				return baseSchema().setColumnType("db1", "users", "id", "INTEGER").build()
			},
			wantOperations: []expectedOperation{},
		},
		{
			// DOUBLE is a SQL alias for Float64
			name: "SQL alias DOUBLE equals canonical Float64",
			from: func() Schema {
				return baseSchema().setColumnType("db1", "users", "id", "DOUBLE").build()
			},
			to: func() Schema {
				return baseSchema().setColumnType("db1", "users", "id", "Float64").build()
			},
			wantOperations: []expectedOperation{},
		},
		{
			// DateTime64 timezone: single quotes stripped in system.columns.type
			name: "DateTime64 with quoted timezone equals unquoted",
			from: func() Schema {
				return baseSchema().setColumnType("db1", "users", "id", "DateTime64(3, UTC)").build()
			},
			to: func() Schema {
				return baseSchema().setColumnType("db1", "users", "id", "DateTime64(3, 'UTC')").build()
			},
			wantOperations: []expectedOperation{},
		},
		{
			// Genuinely different types must still generate MODIFY COLUMN
			name: "genuinely different types generate MODIFY COLUMN",
			from: func() Schema {
				return baseSchema().setColumnType("db1", "users", "id", "Int32").build()
			},
			to: func() Schema {
				return baseSchema().setColumnType("db1", "users", "id", "Int64").build()
			},
			wantOperations: []expectedOperation{
				{level: LevelColumn, action: ActionAlter},
			},
		},
		{
			// '0' and 0 are the same default — system.columns.default_expression varies by server
			name: "quoted numeric default equals unquoted",
			from: func() Schema {
				return baseSchema().setColumnDefault("db1", "users", "id", "'0'").build()
			},
			to: func() Schema {
				return baseSchema().setColumnDefault("db1", "users", "id", "0").build()
			},
			wantOperations: []expectedOperation{},
		},
		{
			// 'false' and false are the same default for Bool columns
			name: "quoted boolean default equals unquoted",
			from: func() Schema {
				return baseSchema().setColumnDefault("db1", "users", "name", "'false'").build()
			},
			to: func() Schema {
				return baseSchema().setColumnDefault("db1", "users", "name", "false").build()
			},
			wantOperations: []expectedOperation{},
		},
		{
			// String defaults like 'hello' must not be incorrectly collapsed
			name: "string default is not equivalent to bare word",
			from: func() Schema {
				return baseSchema().setColumnDefault("db1", "users", "name", "'hello'").build()
			},
			to: func() Schema {
				return baseSchema().setColumnDefault("db1", "users", "name", "hello").build()
			},
			wantOperations: []expectedOperation{
				{level: LevelColumn, action: ActionAlter},
			},
		},
	}

	runTestsWithConfig(t, tests, cfg)
}

// hasOperationWithDestructive checks if an operation exists, optionally checking destructive flag
func hasOperationWithDestructive(strategy Strategy, want expectedOperation) bool {
	for _, op := range strategy.Operations {
		if op.Level == want.level && op.Action == want.action {
			if want.canLoseData != nil && op.CanLoseData != *want.canLoseData {
				continue // canLoseData flag doesn't match
			}
			if want.statements != nil && !reflect.DeepEqual(op.Statements, want.statements) {
				continue // statements don't match
			}
			return true
		}
	}
	return false
}
