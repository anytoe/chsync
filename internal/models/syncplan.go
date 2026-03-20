package models

import (
	"fmt"
	"sort"
	"strings"
)

// SyncPlan represents a schema synchronization plan with multiple complete strategies.
//
// Design rationale:
// When comparing schemas (e.g., instance A has table1, instance B has table11), the same
// atomic differences can be interpreted in multiple ways:
//   - Optimistic: "RENAME table1 TO table11" (if columns are similar)
//   - Conservative: "DROP table1; CREATE table11" (separate operations)
//
// These interpretations are mutually exclusive - you cannot mix operations from different
// strategies. Therefore, we model complete alternative strategies rather than per-difference
// options. This avoids combinatorial explosion while giving users sensible choices.
//
// Users select one complete Strategy, not individual operations.
type SyncPlan struct {
	Strategies []Strategy
}

// Strategy represents one complete interpretation of how to synchronize schemas.
// Examples: "Optimistic (with renames)", "Conservative (drop/create only)", "Hybrid"
type Strategy struct {
	Name        string      // e.g., "Optimistic (with renames)", "Conservative"
	Description string      // human-readable explanation of this strategy
	Operations  []Operation // ordered list of operations to execute
}

// Operation represents a single schema change operation within a strategy.
type Operation struct {
	Level         OperationLevel  // what entity is affected (database, table, column)
	Action        OperationAction // what action is performed (create, drop, rename, alter)
	CanLoseData bool // whether this operation can cause data loss
	Statements    []string        // SQL statements to execute
	Explanation   string          // e.g., "Rename table1→table11 (85% column match)"
}

// OperationLevel identifies what entity a schema operation affects.
type OperationLevel string

const (
	LevelDatabase OperationLevel = "database"
	LevelTable    OperationLevel = "table"
	LevelColumn   OperationLevel = "column"
	LevelIndex    OperationLevel = "index"
	LevelFunction OperationLevel = "function"
)

// OperationAction identifies what action a schema operation performs.
type OperationAction string

const (
	ActionCreate OperationAction = "create"
	ActionDrop   OperationAction = "drop"
	ActionRename OperationAction = "rename"
	ActionAlter  OperationAction = "alter"
)

// SyncPlanGenerator builds migration strategies from a combined schema diff.
type SyncPlanGenerator struct {
	config GeneratorConfig
}

// GeneratorConfig configures how the sync plan generator interprets differences.
type GeneratorConfig struct {
	// Similarity thresholds for detecting potential renames (0.0-1.0)
	TableRenameSimilarityThreshold  float64 // e.g., 0.80 = 80% column match
	ColumnRenameSimilarityThreshold float64

	// TypeAliases maps uppercase alias names to their canonical ClickHouse type names.
	// Loaded at runtime from system.data_type_families via LoadTypeAliases.
	// When set, equivalent types (e.g. INTEGER vs Int32) will not generate MODIFY COLUMN.
	TypeAliases map[string]string
}

// NewSyncPlanGenerator creates a new sync plan generator with the given configuration.
func NewSyncPlanGenerator(config GeneratorConfig) *SyncPlanGenerator {
	return &SyncPlanGenerator{config: config}
}

// Generate creates a SyncPlan with multiple strategies from a combined schema diff.
func (g *SyncPlanGenerator) Generate(combined *CombinedSchema) *SyncPlan {
	plan := &SyncPlan{}
	strategy := g.buildHybridStrategy(combined)
	plan.Strategies = append(plan.Strategies, strategy)
	return plan
}

// buildHybridStrategy creates a strategy that uses renames when similarity is high enough,
// otherwise falls back to drop/create.
func (g *SyncPlanGenerator) buildHybridStrategy(combined *CombinedSchema) Strategy {
	strategy := Strategy{
		Name:        "Hybrid",
		Description: "Uses renames for similar tables/columns, drop/create otherwise",
	}

	var operations []Operation

	// Process each database
	for _, db := range combined.Databases {
		// Handle database-level operations
		switch db.Presence {
		case Target:
			operations = append(operations, Operation{
				Level:         LevelDatabase,
				Action:        ActionCreate,
				CanLoseData: false,
				Statements:    []string{"CREATE DATABASE " + quoteIdent(db.Name) + ";"},
				Explanation:   "Create database " + db.Name,
			})
		case Source:
			operations = append(operations, Operation{
				Level:         LevelDatabase,
				Action:        ActionDrop,
				CanLoseData: true,
				Statements:    []string{"DROP DATABASE IF EXISTS " + quoteIdent(db.Name) + ";"},
				Explanation:   "Drop database " + db.Name,
			})
		case Both:
			// Database exists in both, process tables
			ops := g.processTablesInDatabase(db)
			operations = append(operations, ops...)
		}
	}

	// Process function-level operations
	for _, fn := range combined.Functions {
		switch fn.Presence {
		case Target:
			operations = append(operations, Operation{
				Level:         LevelFunction,
				Action:        ActionCreate,
				CanLoseData: false,
				Statements:    []string{fn.Target.CreateQuery + ";"},
				Explanation:   "Create function " + fn.Name,
			})
		case Source:
			operations = append(operations, Operation{
				Level:         LevelFunction,
				Action:        ActionDrop,
				CanLoseData: true,
				Statements:    []string{"DROP FUNCTION IF EXISTS " + fn.Name + ";"},
				Explanation:   "Drop function " + fn.Name,
			})
		case Both:
			if fn.Source.CreateQuery != fn.Target.CreateQuery {
				operations = append(operations, Operation{
					Level:         LevelFunction,
					Action:        ActionDrop,
					CanLoseData: true,
					Statements:    []string{"DROP FUNCTION IF EXISTS " + fn.Name + ";"},
					Explanation:   "Drop function " + fn.Name + " (body changed)",
				})
				operations = append(operations, Operation{
					Level:         LevelFunction,
					Action:        ActionCreate,
					CanLoseData: false,
					Statements:    []string{fn.Target.CreateQuery + ";"},
					Explanation:   "Create function " + fn.Name,
				})
			}
		}
	}

	strategy.Operations = operations
	return strategy
}

func (g *SyncPlanGenerator) processTablesInDatabase(db CombinedDatabase) []Operation {
	var operations []Operation

	// Collect source-only and target-only tables for rename detection
	var sourceOnlyTables []CombinedTable
	var targetOnlyTables []CombinedTable
	renamedTables := make(map[string]string) // source -> target name mapping

	// First pass: identify tables by presence
	for _, table := range db.Tables {
		switch table.Presence {
		case Source:
			sourceOnlyTables = append(sourceOnlyTables, table)
		case Target:
			targetOnlyTables = append(targetOnlyTables, table)
		}
	}

	// Find rename candidates
	for _, sourceTable := range sourceOnlyTables {
		bestMatch := ""
		bestSimilarity := 0.0

		for _, targetTable := range targetOnlyTables {
			if _, alreadyRenamed := renamedTables[targetTable.Name]; alreadyRenamed {
				continue
			}
			similarity := g.calculateTableSimilarity(&sourceTable, &targetTable)
			if similarity > bestSimilarity && similarity >= g.config.TableRenameSimilarityThreshold {
				bestSimilarity = similarity
				bestMatch = targetTable.Name
			}
		}

		if bestMatch != "" {
			renamedTables[sourceTable.Name] = bestMatch
		}
	}

	// Second pass: generate operations
	for _, table := range db.Tables {
		switch table.Presence {
		case Target:
			// Check if this is a rename target
			if !isRenameTarget(table.Name, renamedTables) {
				operations = append(operations, g.createTableOperation(db.Name, table))
			}
		case Source:
			// Check if this should be renamed
			if newName, isRenamed := renamedTables[table.Name]; isRenamed {
				// Find the target table to check whether properties also changed
				var targetTable *CombinedTable
				for i := range db.Tables {
					if db.Tables[i].Name == newName && db.Tables[i].Presence == Target {
						targetTable = &db.Tables[i]
						break
					}
				}

				// If ENGINE, ORDER BY, or PRIMARY KEY also changed, a rename is not enough: drop + recreate
				needsRecreate := targetTable != nil && table.Source != nil && targetTable.Target != nil &&
					(table.Source.Engine != targetTable.Target.Engine ||
						!equalStringSlices(table.Source.OrderBy, targetTable.Target.OrderBy) ||
						!equalStringSlices(table.Source.PrimaryKey, targetTable.Target.PrimaryKey))

				if needsRecreate {
					operations = append(operations, g.dropTableOperation(db.Name, table))
					operations = append(operations, g.createTableOperation(db.Name, *targetTable))
				} else {
					operations = append(operations, Operation{
						Level:         LevelTable,
						Action:        ActionRename,
						CanLoseData: false,
						Statements:    []string{"RENAME TABLE " + quoteIdent(db.Name) + "." + quoteIdent(table.Name) + " TO " + quoteIdent(db.Name) + "." + quoteIdent(newName) + ";"},
						Explanation:   "Rename table " + table.Name + " to " + newName,
					})
				}
			} else {
				operations = append(operations, g.dropTableOperation(db.Name, table))
			}
		case Both:
			// Table exists in both, check for property changes
			ops := g.processTableChanges(db.Name, table)
			operations = append(operations, ops...)
		}
	}

	return operations
}

func (g *SyncPlanGenerator) processTableChanges(dbName string, table CombinedTable) []Operation {
	var operations []Operation

	// Check if table properties changed (engine, order by, primary key, etc.)
	if table.Source != nil && table.Target != nil {
		if table.Source.Engine != table.Target.Engine ||
			!equalStringSlices(table.Source.OrderBy, table.Target.OrderBy) ||
			!equalStringSlices(table.Source.PrimaryKey, table.Target.PrimaryKey) {
			// ENGINE, ORDER BY, or PRIMARY KEY changed - requires drop + recreate
			operations = append(operations, Operation{
				Level:       LevelTable,
				Action:      ActionDrop,
				CanLoseData: true,
				Statements:  []string{"DROP TABLE IF EXISTS " + quoteIdent(dbName) + "." + quoteIdent(table.Name) + ";"},
				Explanation: "Drop table " + table.Name + " (requires recreation)",
			})
			operations = append(operations, g.createTableOperation(dbName, table))
			return operations
		}
	}

	// Process column-level changes
	ops := g.processColumnsInTable(dbName, table)
	operations = append(operations, ops...)

	return operations
}

func (g *SyncPlanGenerator) processColumnsInTable(dbName string, table CombinedTable) []Operation {
	var operations []Operation

	// Collect source-only and target-only columns for rename detection
	var sourceOnlyColumns []CombinedColumn
	var targetOnlyColumns []CombinedColumn
	renamedColumns := make(map[string]string) // source -> target name mapping

	// First pass: identify columns by presence
	for _, col := range table.Columns {
		switch col.Presence {
		case Source:
			sourceOnlyColumns = append(sourceOnlyColumns, col)
		case Target:
			targetOnlyColumns = append(targetOnlyColumns, col)
		}
	}

	// Find rename candidates
	for _, sourceCol := range sourceOnlyColumns {
		bestMatch := ""
		bestSimilarity := 0.0

		for _, targetCol := range targetOnlyColumns {
			if _, alreadyRenamed := renamedColumns[targetCol.Name]; alreadyRenamed {
				continue
			}
			similarity := g.calculateColumnSimilarityWithNames(sourceCol.Name, sourceCol.Source, targetCol.Name, targetCol.Target)
			if similarity > bestSimilarity && similarity >= g.config.ColumnRenameSimilarityThreshold {
				bestSimilarity = similarity
				bestMatch = targetCol.Name
			}
		}

		if bestMatch != "" {
			renamedColumns[sourceCol.Name] = bestMatch
		}
	}

	// Build target column name list in order (non-source-only), used for FIRST/AFTER clauses.
	var targetCols []string
	for _, col := range table.Columns {
		if col.Presence != Source {
			targetCols = append(targetCols, col.Name)
		}
	}

	// positionClause returns "FIRST" or "AFTER <prev>" for the given column's target position.
	positionClause := func(name string) string {
		for i, n := range targetCols {
			if n == name {
				if i == 0 {
					return "FIRST"
				}
				return "AFTER " + quoteIdent(targetCols[i-1])
			}
		}
		return ""
	}

	// Compute ordinal ranks for position-change detection.
	// Raw Source/Target.Position indices are not directly comparable because they span
	// different column lists (source vs target), causing false positives when columns
	// are added or removed. Instead we build an "effective source order" — Both-columns
	// in source order plus renamed source-only columns inserted at their original positions —
	// and compare ranks restricted to the set of names common to both orderings.
	type srcEntry struct {
		name   string
		srcPos uint64
	}
	var effectiveSrc []srcEntry
	for _, col := range table.Columns {
		switch col.Presence {
		case Both:
			if col.Source != nil {
				effectiveSrc = append(effectiveSrc, srcEntry{col.Name, col.Source.Position})
			}
		case Source:
			if newName, isRenamed := renamedColumns[col.Name]; isRenamed {
				if col.Source != nil {
					effectiveSrc = append(effectiveSrc, srcEntry{newName, col.Source.Position})
				}
			}
		}
	}
	sort.Slice(effectiveSrc, func(i, j int) bool { return effectiveSrc[i].srcPos < effectiveSrc[j].srcPos })
	targetColSet := make(map[string]bool, len(targetCols))
	for _, name := range targetCols {
		targetColSet[name] = true
	}
	effectiveSrcSet := make(map[string]bool, len(effectiveSrc))
	for _, e := range effectiveSrc {
		effectiveSrcSet[e.name] = true
	}
	srcRankAmongCommon := make(map[string]int, len(effectiveSrc))
	r := 0
	for _, e := range effectiveSrc {
		if targetColSet[e.name] {
			srcRankAmongCommon[e.name] = r
			r++
		}
	}
	tgtRankAmongCommon := make(map[string]int, len(targetCols))
	r = 0
	for _, name := range targetCols {
		if effectiveSrcSet[name] {
			tgtRankAmongCommon[name] = r
			r++
		}
	}

	// Second pass: generate operations
	for _, col := range table.Columns {
		switch col.Presence {
		case Target:
			// Check if this is a rename target
			if !isRenameTarget(col.Name, renamedColumns) {
				stmt := "ALTER TABLE " + quoteIdent(dbName) + "." + quoteIdent(table.Name) + " ADD COLUMN " + quoteIdent(col.Name) + " " + col.Target.Type
				if pos := positionClause(col.Name); pos != "" {
					stmt += " " + pos
				}
				operations = append(operations, Operation{
					Level:       LevelColumn,
					Action:      ActionCreate,
					CanLoseData: false,
					Statements:  []string{stmt + ";"},
					Explanation: "Add column " + col.Name + " to " + table.Name,
				})
			}
		case Source:
			// Check if this should be renamed
			if newName, isRenamed := renamedColumns[col.Name]; isRenamed {
				operations = append(operations, Operation{
					Level:       LevelColumn,
					Action:      ActionRename,
					CanLoseData: false,
					Statements:  []string{"ALTER TABLE " + quoteIdent(dbName) + "." + quoteIdent(table.Name) + " RENAME COLUMN " + quoteIdent(col.Name) + " TO " + quoteIdent(newName) + ";"},
					Explanation: "Rename column " + col.Name + " to " + newName,
				})
			} else {
				operations = append(operations, Operation{
					Level:       LevelColumn,
					Action:      ActionDrop,
					CanLoseData: true,
					Statements:  []string{"ALTER TABLE " + quoteIdent(dbName) + "." + quoteIdent(table.Name) + " DROP COLUMN IF EXISTS " + quoteIdent(col.Name) + ";"},
					Explanation: "Drop column " + col.Name + " from " + table.Name + " (mutation — runs in background)",
				})
			}
		case Both:
			// Column exists in both; check for property or position changes
			if col.Source != nil && col.Target != nil {
				if NormalizeColumnType(col.Source.Type, g.config.TypeAliases) != NormalizeColumnType(col.Target.Type, g.config.TypeAliases) || NormalizeDefaultExpression(col.Source.DefaultExpression) != NormalizeDefaultExpression(col.Target.DefaultExpression) {
					var reasons []string
					if NormalizeColumnType(col.Source.Type, g.config.TypeAliases) != NormalizeColumnType(col.Target.Type, g.config.TypeAliases) {
						reasons = append(reasons, "type: "+col.Source.Type+" → "+col.Target.Type+" (mutation, runs in background)")
					}
					if NormalizeDefaultExpression(col.Source.DefaultExpression) != NormalizeDefaultExpression(col.Target.DefaultExpression) {
						srcDefault := col.Source.DefaultExpression
						if srcDefault == "" {
							srcDefault = "(none)"
						}
						tgtDefault := col.Target.DefaultExpression
						if tgtDefault == "" {
							tgtDefault = "(none)"
						}
						reasons = append(reasons, "default: "+srcDefault+" → "+tgtDefault)
					}
					explanation := "Modify column " + col.Name + " in " + table.Name + " [" + strings.Join(reasons, "; ") + "]"
					operations = append(operations, Operation{
						Level:       LevelColumn,
						Action:      ActionAlter,
						CanLoseData: false,
						Statements:  []string{"ALTER TABLE " + quoteIdent(dbName) + "." + quoteIdent(table.Name) + " MODIFY COLUMN " + quoteIdent(col.Name) + " " + col.Target.Type + ";"},
						Explanation: explanation,
					})
				} else if tgtRankAmongCommon[col.Name] < srcRankAmongCommon[col.Name] {
					// Position-only change: column moved to an earlier position in the table.
					// Only forward moves are emitted; columns that shift backward do so automatically
					// once the forward-moving columns are repositioned.
					operations = append(operations, Operation{
						Level:       LevelColumn,
						Action:      ActionAlter,
						CanLoseData: false,
						Statements:  []string{"ALTER TABLE " + quoteIdent(dbName) + "." + quoteIdent(table.Name) + " MODIFY COLUMN " + quoteIdent(col.Name) + " " + col.Target.Type + " " + positionClause(col.Name) + ";"},
						Explanation: "Move column " + col.Name + " in " + table.Name,
					})
				}
			}
		}
	}

	return operations
}

func (g *SyncPlanGenerator) createTableOperation(dbName string, table CombinedTable) Operation {
	createSQL := buildCreateTableSQL(dbName, table)
	return Operation{
		Level:         LevelTable,
		Action:        ActionCreate,
		CanLoseData: false,
		Statements:    []string{createSQL},
		Explanation:   "Create table " + table.Name,
	}
}

func buildCreateTableSQL(dbName string, table CombinedTable) string {
	if table.Target == nil {
		return "CREATE TABLE " + quoteIdent(dbName) + "." + quoteIdent(table.Name) + ";"
	}

	// Collect columns that exist in target
	var colDefs []string
	for _, col := range table.Columns {
		if col.Presence == Source {
			continue
		}
		props := col.Target
		def := quoteIdent(col.Name) + " " + props.Type
		if props.DefaultExpression != "" {
			def += " DEFAULT " + props.DefaultExpression
		}
		if props.CompressionCodec != "" {
			def += " " + props.CompressionCodec
		}
		if props.Comment != "" {
			def += " COMMENT '" + props.Comment + "'"
		}
		colDefs = append(colDefs, def)
	}

	t := table.Target
	sql := fmt.Sprintf("CREATE TABLE %s.%s (%s) ENGINE = %s", quoteIdent(dbName), quoteIdent(table.Name), strings.Join(colDefs, ", "), t.Engine)
	if len(t.OrderBy) > 0 {
		sql += " ORDER BY (" + strings.Join(t.OrderBy, ", ") + ")"
	} else if strings.Contains(t.Engine, "MergeTree") {
		sql += " ORDER BY tuple()"
	}
	if len(t.PrimaryKey) > 0 && !equalStringSlices(t.PrimaryKey, t.OrderBy) {
		sql += " PRIMARY KEY (" + strings.Join(t.PrimaryKey, ", ") + ")"
	}
	if t.PartitionBy != "" {
		sql += " PARTITION BY " + t.PartitionBy
	}
	if len(t.Settings) > 0 {
		keys := make([]string, 0, len(t.Settings))
		for k := range t.Settings {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		pairs := make([]string, len(keys))
		for i, k := range keys {
			pairs[i] = k + " = " + t.Settings[k]
		}
		sql += " SETTINGS " + strings.Join(pairs, ", ")
	}
	return sql + ";"
}

func (g *SyncPlanGenerator) dropTableOperation(dbName string, table CombinedTable) Operation {
	return Operation{
		Level:         LevelTable,
		Action:        ActionDrop,
		CanLoseData: true,
		Statements:    []string{"DROP TABLE IF EXISTS " + quoteIdent(dbName) + "." + quoteIdent(table.Name) + ";"},
		Explanation:   "Drop table " + table.Name,
	}
}

// quoteIdent wraps a ClickHouse identifier in backticks, escaping internal backticks by doubling.
func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func isRenameTarget(name string, renamedMap map[string]string) bool {
	for _, target := range renamedMap {
		if target == name {
			return true
		}
	}
	return false
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// calculateTableSimilarity computes how similar two tables are (0.0-1.0).
// Based on column name overlap, type matches, etc.
func (g *SyncPlanGenerator) calculateTableSimilarity(source, target *CombinedTable) float64 {
	if source == nil || target == nil {
		return 0.0
	}

	// Get column names from both tables
	sourceColNames := make(map[string]bool)
	targetColNames := make(map[string]bool)

	for _, col := range source.Columns {
		if col.Source != nil {
			sourceColNames[col.Name] = true
		}
	}
	for _, col := range target.Columns {
		if col.Target != nil {
			targetColNames[col.Name] = true
		}
	}

	// Calculate Jaccard similarity: |intersection| / |union|
	intersection := 0
	for name := range sourceColNames {
		if targetColNames[name] {
			intersection++
		}
	}

	union := len(sourceColNames) + len(targetColNames) - intersection
	if union == 0 {
		return 0.0
	}

	return float64(intersection) / float64(union)
}

// calculateColumnSimilarity computes how similar two columns are (0.0-1.0).
// Based on type, default values, constraints, etc.
func (g *SyncPlanGenerator) calculateColumnSimilarity(source, target *ColumnProperties) float64 {
	if source == nil || target == nil {
		return 0.0
	}

	// Exact type match gives high similarity
	if source.Type == target.Type {
		return 1.0
	}

	// Different types gives low similarity
	// Could be enhanced to detect compatible types (e.g., Int32 vs Int64)
	return 0.0
}

// calculateColumnSimilarityWithNames computes similarity considering both name and properties.
// This is used for rename detection - columns with very different names shouldn't be renamed.
func (g *SyncPlanGenerator) calculateColumnSimilarityWithNames(sourceName string, source *ColumnProperties, targetName string, target *ColumnProperties) float64 {
	if source == nil || target == nil {
		return 0.0
	}

	// Calculate name similarity (simple substring check)
	nameSimilarity := stringsSimilarity(sourceName, targetName)

	// Calculate property similarity (type match)
	propertySimilarity := g.calculateColumnSimilarity(source, target)

	// Weighted average: name similarity is more important for column renames
	// If names are completely different, don't suggest rename even if types match
	return 0.7*nameSimilarity + 0.3*propertySimilarity
}

// stringsSimilarity computes a simple similarity score between two strings.
// Returns 1.0 if one string contains the other, 0.0 if completely different.
func stringsSimilarity(a, b string) float64 {
	if a == b {
		return 1.0
	}

	// Check if one contains the other (case insensitive)
	aLower := stringToLower(a)
	bLower := stringToLower(b)

	if stringContains(aLower, bLower) || stringContains(bLower, aLower) {
		return 0.8
	}

	return 0.0
}

func stringToLower(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			result[i] = s[i] + ('a' - 'A')
		} else {
			result[i] = s[i]
		}
	}
	return string(result)
}

func stringContains(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(needle) > len(haystack) {
		return false
	}
	for i := 0; i <= len(haystack)-len(needle); i++ {
		match := true
		for j := 0; j < len(needle); j++ {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
