package models

import (
	"testing"
)

func TestCleanupSharedMergeTree(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "SharedMergeTree with no parameters",
			input:    "ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')",
			expected: "ENGINE = MergeTree()",
		},
		{
			name:     "SharedReplacingMergeTree with parameter",
			input:    "ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', modified_at)",
			expected: "ENGINE = ReplacingMergeTree(modified_at)",
		},
		{
			name:     "SharedAggregatingMergeTree with parameter",
			input:    "ENGINE = SharedAggregatingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)",
			expected: "ENGINE = AggregatingMergeTree(version)",
		},
		{
			name:     "SharedCollapsingMergeTree with parameter",
			input:    "ENGINE = SharedCollapsingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', sign)",
			expected: "ENGINE = CollapsingMergeTree(sign)",
		},
		{
			name:     "Regular MergeTree unchanged",
			input:    "ENGINE = MergeTree() ORDER BY id",
			expected: "ENGINE = MergeTree() ORDER BY id",
		},
		{
			name:     "ReplacingMergeTree without Shared prefix unchanged",
			input:    "ENGINE = ReplacingMergeTree(updated_at) ORDER BY id",
			expected: "ENGINE = ReplacingMergeTree(updated_at) ORDER BY id",
		},
		{
			name: "Full CREATE TABLE with SharedReplacingMergeTree",
			input: `CREATE TABLE db.table (
    id UInt64,
    modified_at DateTime
) ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', modified_at)
ORDER BY id;`,
			expected: `CREATE TABLE db.table (
    id UInt64,
    modified_at DateTime
) ENGINE = ReplacingMergeTree(modified_at)
ORDER BY id;`,
		},
		{
			name: "Full CREATE TABLE with SharedMergeTree no params",
			input: `CREATE TABLE db.events (
    event_id UInt64,
    timestamp DateTime
) ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY event_id;`,
			expected: `CREATE TABLE db.events (
    event_id UInt64,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY event_id;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cleanupSharedMergeTree(tt.input)
			if result != tt.expected {
				t.Errorf("\nExpected:\n%s\n\nGot:\n%s", tt.expected, result)
			}
		})
	}
}

func TestAddTimeTypeSetting(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name: "Time type with no existing SETTINGS",
			input: `CREATE TABLE db.table (
    id UInt64,
    time_col Time
) ENGINE = MergeTree() ORDER BY id;`,
			expected: `CREATE TABLE db.table (
    id UInt64,
    time_col Time
) ENGINE = MergeTree() ORDER BY id SETTINGS enable_time_time64_type = 1;`,
		},
		{
			name: "Time type with existing SETTINGS",
			input: `CREATE TABLE db.table (
    id UInt64,
    time_col Time
) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 8192;`,
			expected: `CREATE TABLE db.table (
    id UInt64,
    time_col Time
) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 8192, enable_time_time64_type = 1;`,
		},
		{
			name: "No Time type - no modification",
			input: `CREATE TABLE db.table (
    id UInt64,
    timestamp DateTime
) ENGINE = MergeTree() ORDER BY id;`,
			expected: `CREATE TABLE db.table (
    id UInt64,
    timestamp DateTime
) ENGINE = MergeTree() ORDER BY id;`,
		},
		{
			name: "DateTime contains Time but not Time type - no modification",
			input: `CREATE TABLE db.table (
    id UInt64,
    timestamp DateTime
) ENGINE = MergeTree() ORDER BY id;`,
			expected: `CREATE TABLE db.table (
    id UInt64,
    timestamp DateTime
) ENGINE = MergeTree() ORDER BY id;`,
		},
		{
			name: "Multiple Time columns with existing SETTINGS",
			input: `CREATE TABLE db.table (
    id UInt64,
    start_time Time,
    end_time Time
) ENGINE = MergeTree() ORDER BY id SETTINGS ttl_only_drop_parts = 1;`,
			expected: `CREATE TABLE db.table (
    id UInt64,
    start_time Time,
    end_time Time
) ENGINE = MergeTree() ORDER BY id SETTINGS ttl_only_drop_parts = 1, enable_time_time64_type = 1;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := addTimeTypeSetting(tt.input)
			if result != tt.expected {
				t.Errorf("\nExpected:\n%s\n\nGot:\n%s", tt.expected, result)
			}
		})
	}
}

func TestSQLStatements_Add(t *testing.T) {
	tests := []struct {
		name              string
		input             string
		expectedCleaned   string
	}{
		{
			name:  "SharedMergeTree gets cleaned",
			input: "CREATE TABLE test ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}');",
			expectedCleaned: "CREATE TABLE test ENGINE = MergeTree();",
		},
		{
			name:  "Time type gets setting added",
			input: "CREATE TABLE test (t Time) ENGINE = MergeTree();",
			expectedCleaned: "CREATE TABLE test (t Time) ENGINE = MergeTree() SETTINGS enable_time_time64_type = 1;",
		},
		{
			name:  "Both transformations applied",
			input: "CREATE TABLE test (t Time) ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}');",
			expectedCleaned: "CREATE TABLE test (t Time) ENGINE = MergeTree() SETTINGS enable_time_time64_type = 1;",
		},
		{
			name:  "No transformations needed",
			input: "CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY id;",
			expectedCleaned: "CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY id;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts := &SQLStatements{}
			stmts.Add(tt.input)

			if len(stmts.StatementsCleaned) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts.StatementsCleaned))
			}

			if stmts.StatementsCleaned[0] != tt.expectedCleaned {
				t.Errorf("\nExpected:\n%s\n\nGot:\n%s", tt.expectedCleaned, stmts.StatementsCleaned[0])
			}
		})
	}
}

func TestSQLStatements_ToStatements(t *testing.T) {
	tests := []struct {
		name       string
		version    string
		statements []string
		contains   []string // strings that should be in the output
	}{
		{
			name:    "Single statement with version",
			version: "24.1.1.1",
			statements: []string{
				"CREATE DATABASE test;",
			},
			contains: []string{
				"Version: 24.1.1.1",
				"CREATE DATABASE test;",
			},
		},
		{
			name:    "Multiple statements with proper spacing",
			version: "24.1.1.1",
			statements: []string{
				"CREATE DATABASE db1;",
				"CREATE DATABASE db2;",
				"CREATE TABLE db1.table1 (id UInt64) ENGINE = MergeTree() ORDER BY id;",
			},
			contains: []string{
				"CREATE DATABASE db1;",
				"CREATE DATABASE db2;",
				"CREATE TABLE db1.table1",
			},
		},
		{
			name:       "Empty statements",
			version:    "24.1.1.1",
			statements: []string{},
			contains: []string{
				"Version: 24.1.1.1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts := &SQLStatements{
				Version:           tt.version,
				StatementsCleaned: tt.statements,
			}

			result := stmts.ToStatements()

			// Check header is present
			if !contains(result, "--------------------------------------------------") {
				t.Error("Expected header dashes in output")
			}

			// Check all expected strings are present
			for _, expected := range tt.contains {
				if !contains(result, expected) {
					t.Errorf("Expected output to contain: %s\n\nGot:\n%s", expected, result)
				}
			}

			// Check double newlines between statements if multiple statements
			if len(tt.statements) > 1 {
				if !contains(result, "\n\n") {
					t.Error("Expected double newlines between statements")
				}
			}
		})
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsAt(s, substr))
}

func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
