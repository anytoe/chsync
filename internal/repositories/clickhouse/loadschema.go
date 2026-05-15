package clickhouse

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/anytoe/chsync/internal/models"
)

var excludedDatabases = []string{
	"INFORMATION_SCHEMA",
	"information_schema",
	"system",
}

// Filter controls which databases and tables are loaded or exported.
// System databases (system, INFORMATION_SCHEMA, etc.) are always excluded.
//
// IncludeDbs/IncludeTables act as allowlists; when non-empty, everything else is ignored.
// ExcludeDbs/ExcludeTables act as denylists on top of whatever the include filter admits.
// Tables are matched by name only; use IncludeDbs/ExcludeDbs to scope by database.
type Filter struct {
	OnlyDbs    []string
	SkipDbs    []string
	OnlyTables []string
	SkipTables []string
}

// LoadTypeAliases queries system.data_type_families and returns a map of
// uppercase alias name → canonical type name. Use this to populate GeneratorConfig.TypeAliases
// so that equivalent types (e.g. INTEGER vs Int32) are not flagged as differences.
func (c *Client) LoadTypeAliases(ctx context.Context) (map[string]string, error) {
	rows, err := c.Query(ctx, "SELECT name, alias_to FROM system.data_type_families WHERE alias_to != ''")
	if err != nil {
		return nil, fmt.Errorf("query type aliases: %w", err)
	}
	defer rows.Close()

	aliases := make(map[string]string)
	for rows.Next() {
		var name, aliasTo string
		if err := rows.Scan(&name, &aliasTo); err != nil {
			return nil, fmt.Errorf("scan type alias: %w", err)
		}
		aliases[strings.ToUpper(name)] = aliasTo
	}
	return aliases, rows.Err()
}

// LoadSchema queries ClickHouse system tables and returns the full schema.
// Uses exactly 3 queries: databases, tables, columns — then assembles in memory.
func (c *Client) LoadSchema(ctx context.Context, f Filter) (*models.Schema, error) {
	databases, err := c.loadDatabases(ctx, f)
	if err != nil {
		return nil, fmt.Errorf("load databases: %w", err)
	}

	tables, err := c.loadTables(ctx, databases, f)
	if err != nil {
		return nil, fmt.Errorf("load tables: %w", err)
	}

	columns, err := c.loadColumns(ctx, databases, f)
	if err != nil {
		return nil, fmt.Errorf("load columns: %w", err)
	}

	projections, err := c.loadProjections(ctx, databases, f)
	if err != nil {
		return nil, fmt.Errorf("load projections: %w", err)
	}

	functions, err := c.loadFunctions(ctx)
	if err != nil {
		return nil, fmt.Errorf("load functions: %w", err)
	}

	dictionaries, err := c.loadDictionaries(ctx, databases, f)
	if err != nil {
		return nil, fmt.Errorf("load dictionaries: %w", err)
	}

	materializedViews, err := c.loadMaterializedViews(ctx, databases, f)
	if err != nil {
		return nil, fmt.Errorf("load materialized views: %w", err)
	}

	// Assign columns and projections to their tables
	for key, cols := range columns {
		if t, ok := tables[key]; ok {
			t.Columns = cols
			tables[key] = t
		}
	}
	for key, projs := range projections {
		if t, ok := tables[key]; ok {
			t.Projections = projs
			tables[key] = t
		}
	}

	// Group tables by database
	schema := &models.Schema{}
	for _, dbName := range databases {
		db := models.Database{Name: dbName}
		for key, t := range tables {
			if strings.HasPrefix(key, dbName+".") {
				db.Tables = append(db.Tables, t)
			}
		}
		schema.Databases = append(schema.Databases, db)
	}

	schema.Functions = functions
	schema.Dictionaries = dictionaries
	schema.MaterializedViews = materializedViews

	return schema, nil
}

// loadMaterializedViews reads CREATE MATERIALIZED VIEW statements verbatim
// from system.tables (where engine = 'MaterializedView') for the given
// databases. The full create_table_query is kept because the AS SELECT body
// and the TO target clause cannot be recovered from column metadata alone.
func (c *Client) loadMaterializedViews(ctx context.Context, databases []string, f Filter) ([]models.MaterializedView, error) {
	if len(databases) == 0 {
		return nil, nil
	}
	cond := fmt.Sprintf("database IN (%s) AND engine = 'MaterializedView'", quoted(databases))
	cond += tableFilterClauses(f, "name")

	query := fmt.Sprintf(
		"SELECT database, name, create_table_query FROM system.tables WHERE %s ORDER BY database, name",
		cond,
	)
	rows, err := c.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query materialized views: %w", err)
	}
	defer rows.Close()

	var mvs []models.MaterializedView
	for rows.Next() {
		var m models.MaterializedView
		if err := rows.Scan(&m.Database, &m.Name, &m.CreateQuery); err != nil {
			return nil, fmt.Errorf("scan materialized view: %w", err)
		}
		mvs = append(mvs, m)
	}
	return mvs, rows.Err()
}

// loadDictionaries reads CREATE DICTIONARY statements verbatim from
// system.tables (where engine = 'Dictionary') for the given databases.
// The full create_table_query is kept; bodies are not parsed.
func (c *Client) loadDictionaries(ctx context.Context, databases []string, f Filter) ([]models.Dictionary, error) {
	if len(databases) == 0 {
		return nil, nil
	}
	cond := fmt.Sprintf("database IN (%s) AND engine = 'Dictionary'", quoted(databases))
	cond += tableFilterClauses(f, "name")

	query := fmt.Sprintf(
		"SELECT database, name, create_table_query FROM system.tables WHERE %s ORDER BY database, name",
		cond,
	)
	rows, err := c.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query dictionaries: %w", err)
	}
	defer rows.Close()

	var dicts []models.Dictionary
	for rows.Next() {
		var d models.Dictionary
		if err := rows.Scan(&d.Database, &d.Name, &d.CreateQuery); err != nil {
			return nil, fmt.Errorf("scan dictionary: %w", err)
		}
		dicts = append(dicts, d)
	}
	return dicts, rows.Err()
}

// loadFunctions loads all SQL user-defined functions.
func (c *Client) loadFunctions(ctx context.Context) ([]models.Function, error) {
	rows, err := c.Query(ctx, "SELECT name, create_query FROM system.functions WHERE origin = 'SQLUserDefined' ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("query functions: %w", err)
	}
	defer rows.Close()

	var functions []models.Function
	for rows.Next() {
		var fn models.Function
		if err := rows.Scan(&fn.Name, &fn.CreateQuery); err != nil {
			return nil, fmt.Errorf("scan function: %w", err)
		}
		functions = append(functions, fn)
	}
	return functions, rows.Err()
}

func (c *Client) loadDatabases(ctx context.Context, f Filter) ([]string, error) {
	excluded := append(excludedDatabases, f.SkipDbs...)
	excludeClause := quoted(excluded)

	var query string
	if len(f.OnlyDbs) > 0 {
		query = fmt.Sprintf(
			"SELECT name FROM system.databases WHERE name IN (%s) AND name NOT IN (%s) ORDER BY name",
			quoted(f.OnlyDbs), excludeClause,
		)
	} else {
		query = fmt.Sprintf(
			"SELECT name FROM system.databases WHERE name NOT IN (%s) ORDER BY name",
			excludeClause,
		)
	}

	rows, err := c.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query databases: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan database: %w", err)
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// loadTables loads all tables for the given databases in a single query.
// Returns a map keyed by "database.table".
func (c *Client) loadTables(ctx context.Context, databases []string, f Filter) (map[string]models.Table, error) {
	cond := fmt.Sprintf("database IN (%s) AND engine NOT IN ('Dictionary', 'MaterializedView')", quoted(databases))
	cond += tableFilterClauses(f, "name")

	query := fmt.Sprintf(
		"SELECT database, name, engine, engine_full, sorting_key, primary_key, partition_key FROM system.tables WHERE %s ORDER BY database, name",
		cond,
	)

	rows, err := c.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query tables: %w", err)
	}
	defer rows.Close()

	tables := make(map[string]models.Table)
	for rows.Next() {
		var dbName, name, engine, engineFull, sortingKey, primaryKey, partitionKey string
		if err := rows.Scan(&dbName, &name, &engine, &engineFull, &sortingKey, &primaryKey, &partitionKey); err != nil {
			return nil, fmt.Errorf("scan table: %w", err)
		}

		table := models.Table{
			Name:        name,
			Engine:      models.NormalizeEngine(engine),
			EngineArgs:  parseEngineArgs(engineFull),
			PartitionBy: partitionKey,
			Settings:    parseSettings(engineFull),
		}
		if sortingKey != "" {
			table.OrderBy = strings.Split(sortingKey, ", ")
		}
		if primaryKey != "" && primaryKey != sortingKey {
			table.PrimaryKey = strings.Split(primaryKey, ", ")
		}

		tables[dbName+"."+name] = table
	}
	return tables, rows.Err()
}

// replicationParamsRe matches the two leading replication arguments that
// Shared*/Replicated* MergeTree engines carry inside their engine arg list:
//
//	'/clickhouse/tables/{uuid}/{shard}', '{replica}'[, <real args>]
//
// These are infrastructure parameters managed by ClickHouse Cloud and not part
// of the logical table definition, so they are stripped to mirror
// NormalizeEngine which already strips the "Shared" prefix from the engine name.
var replicationParamsRe = regexp.MustCompile(`^'/clickhouse/tables/\{uuid\}/\{shard\}',\s*'\{replica\}'(?:,\s*)?`)

// parseEngineArgs extracts the parenthesized engine arguments from a
// system.tables engine_full string. ClickHouse formats engine_full as
// "<Engine>(<args>) [ORDER BY ...] [PARTITION BY ...] [SETTINGS ...]"; an
// engine reported with no args (e.g. "MergeTree") has no parens at all and
// this returns "".
//
// The version column on engines like ReplacingMergeTree(xo_received_at) lives
// here and would be lost without this — system.tables.engine returns only the
// bare engine name.
func parseEngineArgs(engineFull string) string {
	open := strings.IndexByte(engineFull, '(')
	if open < 0 {
		return ""
	}
	depth := 0
	inQuote := false
	close := -1
	for i := open; i < len(engineFull); i++ {
		ch := engineFull[i]
		if ch == '\'' {
			inQuote = !inQuote
			continue
		}
		if inQuote {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				close = i
			}
		}
		if close != -1 {
			break
		}
	}
	if close < 0 {
		return ""
	}
	args := engineFull[open+1 : close]
	return replicationParamsRe.ReplaceAllString(args, "")
}

// parseSettings extracts the SETTINGS clause from a system.tables engine_full
// string and returns it as a key→value map. ClickHouse formats engine_full as
// "<Engine>(...) [ORDER BY ...] [PARTITION BY ...] [SETTINGS k1 = v1, k2 = v2, ...]".
// Returns nil if there is no SETTINGS clause.
//
// Values are taken verbatim (e.g. "8192", "'foo'") — not unquoted — so they
// round-trip into the CREATE TABLE SETTINGS clause unchanged.
func parseSettings(engineFull string) map[string]string {
	const marker = " SETTINGS "
	idx := strings.Index(engineFull, marker)
	if idx == -1 {
		return nil
	}
	rest := engineFull[idx+len(marker):]

	result := make(map[string]string)
	// Split on commas that sit outside single-quoted string values.
	var current strings.Builder
	inQuote := false
	flush := func() {
		pair := strings.TrimSpace(current.String())
		current.Reset()
		if pair == "" {
			return
		}
		eq := strings.Index(pair, "=")
		if eq < 0 {
			return
		}
		key := strings.TrimSpace(pair[:eq])
		val := strings.TrimSpace(pair[eq+1:])
		if key != "" {
			result[key] = val
		}
	}
	for i := 0; i < len(rest); i++ {
		ch := rest[i]
		if ch == '\'' {
			inQuote = !inQuote
		}
		if ch == ',' && !inQuote {
			flush()
			continue
		}
		current.WriteByte(ch)
	}
	flush()

	if len(result) == 0 {
		return nil
	}
	return result
}

// loadProjections loads all table projections for the given databases.
// Returns a map keyed by "database.table" → (projection_name → body).
// The body is the SELECT clause as ClickHouse stores it in system.projections.query.
func (c *Client) loadProjections(ctx context.Context, databases []string, f Filter) (map[string]map[string]string, error) {
	if len(databases) == 0 {
		return nil, nil
	}
	cond := fmt.Sprintf("database IN (%s)", quoted(databases))
	cond += tableFilterClauses(f, "table")

	query := fmt.Sprintf(
		"SELECT database, table, name, query FROM system.projections WHERE %s ORDER BY database, table, name",
		cond,
	)
	rows, err := c.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query projections: %w", err)
	}
	defer rows.Close()

	result := make(map[string]map[string]string)
	for rows.Next() {
		var dbName, tableName, name, body string
		if err := rows.Scan(&dbName, &tableName, &name, &body); err != nil {
			return nil, fmt.Errorf("scan projection: %w", err)
		}
		key := dbName + "." + tableName
		if result[key] == nil {
			result[key] = make(map[string]string)
		}
		result[key][name] = body
	}
	return result, rows.Err()
}

// loadColumns loads all columns for the given databases in a single query.
// Returns a map keyed by "database.table".
func (c *Client) loadColumns(ctx context.Context, databases []string, f Filter) (map[string][]models.Column, error) {
	cond := fmt.Sprintf("database IN (%s)", quoted(databases))
	cond += tableFilterClauses(f, "table") // system.columns uses "table", not "name"

	query := fmt.Sprintf(
		"SELECT database, table, name, type, default_expression, compression_codec, comment FROM system.columns WHERE %s ORDER BY database, table, position",
		cond,
	)

	rows, err := c.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query columns: %w", err)
	}
	defer rows.Close()

	columns := make(map[string][]models.Column)
	for rows.Next() {
		var dbName, tableName string
		var col models.Column
		if err := rows.Scan(&dbName, &tableName, &col.Name, &col.Type, &col.DefaultExpression, &col.CompressionCodec, &col.Comment); err != nil {
			return nil, fmt.Errorf("scan column: %w", err)
		}
		key := dbName + "." + tableName
		columns[key] = append(columns[key], col)
	}
	return columns, rows.Err()
}

// quoted returns a SQL IN-list string: 'a','b','c'
func quoted(ss []string) string {
	return "'" + strings.Join(ss, "','") + "'"
}

// tableFilterClauses returns SQL AND clauses filtering by table name.
// col is the column name that holds the table name in the queried system table (e.g. "name" or "table").
func tableFilterClauses(f Filter, col string) string {
	var s string
	if len(f.OnlyTables) > 0 {
		s += fmt.Sprintf(" AND %s IN (%s)", col, quoted(f.OnlyTables))
	}
	if len(f.SkipTables) > 0 {
		s += fmt.Sprintf(" AND %s NOT IN (%s)", col, quoted(f.SkipTables))
	}
	return s
}
