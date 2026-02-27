package models

import (
	"fmt"
	"regexp"
	"strings"
)

type SQLStatements struct {
	Version           string
	StatementsCleaned []string
}

// ParseFile splits raw SQL file content into individual CREATE statements.
// Returns an error if the file is not a valid chsync schema snapshot (missing header).
func ParseFile(content string) (*SQLStatements, error) {
	if !strings.HasPrefix(content, "--------------------------------------------------\n--- Version:") {
		return nil, fmt.Errorf("not a valid chsync schema snapshot (missing header)\nUse 'chsync snapshot' to produce a compatible schema file")
	}

	s := &SQLStatements{}
	var current strings.Builder
	for _, line := range strings.Split(content, "\n") {
		current.WriteString(line)
		current.WriteByte('\n')
		trimmed := strings.TrimRight(line, " \t\r")
		if strings.HasSuffix(trimmed, ";") {
			s.StatementsCleaned = append(s.StatementsCleaned, current.String())
			current.Reset()
		}
	}
	return s, nil
}

func (stmts *SQLStatements) Add(s string) {
	stmt := cleanupSharedMergeTree(s)
	stmt = addTimeTypeSetting(stmt)
	stmts.StatementsCleaned = append(stmts.StatementsCleaned, stmt)
}

func (stmts *SQLStatements) ToStatements() string {
	b := strings.Builder{}

	b.WriteString(stmts.header())

	for _, s := range stmts.StatementsCleaned {
		b.WriteString(s)
		b.WriteString("\n\n")
	}

	b.WriteString("\n")

	return b.String()
}

func (stmts *SQLStatements) header() string {
	return fmt.Sprintf("--------------------------------------------------\n--- Version: %s ------------------------\n--------------------------------------------------\n\n", stmts.Version)
}

// cleanupSharedMergeTree removes Shared prefix and replication parameters from engine definitions
// Transforms:
// - SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') -> MergeTree()
// - SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', modified_at) -> ReplacingMergeTree(modified_at)
func cleanupSharedMergeTree(sql string) string {
	// Case 1: With additional parameters after replica (has comma after replica)
	// Remove Shared prefix and replication params, keep everything after the comma (and any trailing space)
	patternWithParams := regexp.MustCompile(`Shared(\w*MergeTree)\('/clickhouse/tables/\{uuid\}/\{shard\}', '\{replica\}',\s*`)
	sql = patternWithParams.ReplaceAllString(sql, `$1(`)

	// Case 2: Without additional parameters (no comma after replica)
	// Remove Shared prefix and replication params, leave empty parens
	patternNoParams := regexp.MustCompile(`Shared(\w*MergeTree)\('/clickhouse/tables/\{uuid\}/\{shard\}', '\{replica\}'\)`)
	sql = patternNoParams.ReplaceAllString(sql, `$1()`)

	return sql
}

// addTimeTypeSetting adds enable_time_time64_type = 1 setting for tables with Time data type
func addTimeTypeSetting(sql string) string {
	// Check if the CREATE TABLE has a Time data type
	hasTimeType := regexp.MustCompile(`\bTime\b`).MatchString(sql)
	if !hasTimeType {
		return sql
	}

	// Check if SETTINGS already exists
	settingsPattern := regexp.MustCompile(`SETTINGS\s+(.+?);`)
	if settingsPattern.MatchString(sql) {
		// Append to existing SETTINGS
		return settingsPattern.ReplaceAllString(sql, `SETTINGS $1, enable_time_time64_type = 1;`)
	}

	// Add new SETTINGS clause before the semicolon
	return regexp.MustCompile(`;`).ReplaceAllString(sql, ` SETTINGS enable_time_time64_type = 1;`)
}
