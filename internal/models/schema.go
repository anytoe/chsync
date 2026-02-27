package models

import (
	"regexp"
	"strconv"
	"strings"
)

// Schema represents a complete ClickHouse schema
type Schema struct {
	Databases []Database
	Functions []Function
}

// Function represents a SQL user-defined function
type Function struct {
	Name        string
	CreateQuery string
}

// Database represents a ClickHouse database with its tables
type Database struct {
	Name   string
	Tables []Table
}

// Table represents a ClickHouse table with its columns
// Includes all table types: Table, View, MaterializedView, Dictionary, etc.
type Table struct {
	Name        string
	Engine      string
	Columns     []Column
	OrderBy     []string
	PrimaryKey  []string
	PartitionBy string
	Settings    map[string]string
}

// NormalizeEngine strips cloud-specific prefixes from engine names.
// e.g. SharedMergeTree -> MergeTree, SharedReplacingMergeTree -> ReplacingMergeTree
func NormalizeEngine(engine string) string {
	return strings.TrimPrefix(engine, "Shared")
}

var columnTypeTZQuoteRe = regexp.MustCompile(`,\s*'([\w/+\-]+)'`)

// NormalizeDefaultExpression canonicalizes default expression representations.
// ClickHouse may quote simple literals differently across versions:
// '0' and 0 are equivalent for numeric columns; 'false' and false for Bool.
// Outer quotes are stripped only for simple numeric and boolean literals — complex
// string defaults like 'hello' are left untouched to avoid false equivalences.
func NormalizeDefaultExpression(expr string) string {
	if len(expr) < 2 || expr[0] != '\'' || expr[len(expr)-1] != '\'' {
		return expr
	}
	inner := expr[1 : len(expr)-1]
	if strings.Contains(inner, "'") {
		return expr // contains embedded quotes — not a simple literal
	}
	if inner == "true" || inner == "false" {
		return inner
	}
	if _, err := strconv.ParseFloat(inner, 64); err == nil {
		return inner
	}
	return expr
}

// NormalizeColumnType resolves ClickHouse type aliases and formatting variants so that
// equivalent types compare equal.
//
// aliases is a map of uppercase alias name → canonical type name, as returned by
// LoadTypeAliases (queried from system.data_type_families). Pass nil to skip alias resolution.
//
// Two normalizations are always applied regardless of the alias map:
//   - DateTime64/DateTime timezone quotes: DateTime64(3, 'UTC') → DateTime64(3, UTC)
func NormalizeColumnType(t string, aliases map[string]string) string {
	// Strip quotes from timezone names inside DateTime/DateTime64 types.
	// system.columns.type returns DateTime64(3, UTC) but DDL often uses DateTime64(3, 'UTC').
	if strings.HasPrefix(t, "DateTime") {
		t = columnTypeTZQuoteRe.ReplaceAllString(t, ", $1")
	}
	// Resolve SQL-style aliases (case-insensitive) to their canonical ClickHouse type.
	if len(aliases) > 0 {
		if canonical, ok := aliases[strings.ToUpper(t)]; ok {
			return canonical
		}
	}
	return t
}

// Column represents a table column
type Column struct {
	Name              string
	Type              string
	DefaultExpression string
	CompressionCodec  string
	Comment           string
}
