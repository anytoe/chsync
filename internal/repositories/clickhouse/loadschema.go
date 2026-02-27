package clickhouse

import (
	"context"
	"fmt"
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

	functions, err := c.loadFunctions(ctx)
	if err != nil {
		return nil, fmt.Errorf("load functions: %w", err)
	}

	// Assign columns to their tables
	for key, cols := range columns {
		if t, ok := tables[key]; ok {
			t.Columns = cols
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

	return schema, nil
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
	cond := fmt.Sprintf("database IN (%s)", quoted(databases))
	cond += tableFilterClauses(f, "name")

	query := fmt.Sprintf(
		"SELECT database, name, engine, sorting_key, partition_key FROM system.tables WHERE %s ORDER BY database, name",
		cond,
	)

	rows, err := c.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query tables: %w", err)
	}
	defer rows.Close()

	tables := make(map[string]models.Table)
	for rows.Next() {
		var dbName, name, engine, sortingKey, partitionKey string
		if err := rows.Scan(&dbName, &name, &engine, &sortingKey, &partitionKey); err != nil {
			return nil, fmt.Errorf("scan table: %w", err)
		}

		table := models.Table{
			Name:        name,
			Engine:      models.NormalizeEngine(engine),
			PartitionBy: partitionKey,
		}
		if sortingKey != "" {
			table.OrderBy = strings.Split(sortingKey, ", ")
		}

		tables[dbName+"."+name] = table
	}
	return tables, rows.Err()
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
