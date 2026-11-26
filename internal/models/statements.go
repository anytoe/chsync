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

// cleanupSharedMergeTree removes the ClickHouse Cloud "Shared" prefix and
// replication parameters from engine definitions, so the snapshot replays on
// a plain ClickHouse server. Transforms:
// - SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') -> MergeTree()
// - SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', modified_at) -> ReplacingMergeTree(modified_at)
// - SharedJoin(ANY, LEFT, k) -> Join(ANY, LEFT, k), SharedSet -> Set
func cleanupSharedMergeTree(sql string) string {
	// Case 1: With additional parameters after replica (has comma after replica)
	// Remove Shared prefix and replication params, keep everything after the comma (and any trailing space)
	patternWithParams := regexp.MustCompile(`Shared(\w*MergeTree)\('/clickhouse/tables/\{uuid\}/\{shard\}', '\{replica\}',\s*`)
	sql = patternWithParams.ReplaceAllString(sql, `$1(`)

	// Case 2: Without additional parameters (no comma after replica)
	// Remove Shared prefix and replication params, leave empty parens
	patternNoParams := regexp.MustCompile(`Shared(\w*MergeTree)\('/clickhouse/tables/\{uuid\}/\{shard\}', '\{replica\}'\)`)
	sql = patternNoParams.ReplaceAllString(sql, `$1()`)

	// Case 3: Cloud-only Join/Set engines carry no replication params.
	// SharedJoin(ANY, LEFT, k) -> Join(ANY, LEFT, k), SharedSet -> Set
	patternJoinSet := regexp.MustCompile(`ENGINE = Shared(Join|Set)\b`)
	sql = patternJoinSet.ReplaceAllString(sql, `ENGINE = $1`)

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

var queryParameterPattern = regexp.MustCompile(`^\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*([^{}]*?)\s*\}`)

// NormalizeQueryParameters removes the whitespace around the name and the type
// of each query parameter: "{ ids:Array(UInt32) }" -> "{ids:Array(UInt32)}".
// The SQL formatter adds this whitespace, and ClickHouse then keeps it as part
// of the parameter type. Quoted strings and identifiers stay as they are.
func NormalizeQueryParameters(sql string) string {
	var b strings.Builder
	for i := 0; i < len(sql); {
		switch c := sql[i]; c {
		case '\'', '"', '`':
			end := quotedEnd(sql, i)
			b.WriteString(sql[i:end])
			i = end
		case '{':
			if m := queryParameterPattern.FindStringSubmatch(sql[i:]); m != nil {
				b.WriteString("{" + m[1] + ":" + m[2] + "}")
				i += len(m[0])
				continue
			}
			fallthrough
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

// quotedEnd returns the index after the quoted text that starts at sql[start].
func quotedEnd(sql string, start int) int {
	quote := sql[start]
	for i := start + 1; i < len(sql); i++ {
		switch sql[i] {
		case '\\':
			i++
		case quote:
			return i + 1
		}
	}
	return len(sql)
}
