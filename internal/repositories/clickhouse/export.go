package clickhouse

import (
	"context"
	"fmt"

	"github.com/anytoe/chsync/internal/models"
)

// ExportSQL exports the schema as SQL statements in the correct order.
func (e *Client) ExportSQL(ctx context.Context, f Filter) (*models.SQLStatements, error) {
	// Get ClickHouse version
	versionRows, err := e.Query(ctx, "SELECT version()")
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}
	defer versionRows.Close()

	var version string
	if versionRows.Next() {
		if err := versionRows.Scan(&version); err != nil {
			return nil, fmt.Errorf("failed to scan version: %w", err)
		}
	}
	versionRows.Close()

	allSkippedDbs := append(excludedDatabases, f.SkipDbs...)
	skipDbClause := quoted(allSkippedDbs)

	var dbFilter string
	if len(f.OnlyDbs) > 0 {
		dbFilter = fmt.Sprintf("name IN (%s) AND name NOT IN (%s)", quoted(f.OnlyDbs), skipDbClause)
	} else {
		dbFilter = fmt.Sprintf("name NOT IN (%s)", skipDbClause)
	}

	var tableDbFilter string
	if len(f.OnlyDbs) > 0 {
		tableDbFilter = fmt.Sprintf("database IN (%s) AND database NOT IN (%s)", quoted(f.OnlyDbs), skipDbClause)
	} else {
		tableDbFilter = fmt.Sprintf("database NOT IN (%s)", skipDbClause)
	}
	tableDbFilter += tableFilterClauses(f, "name")

	query := fmt.Sprintf(`
SELECT stmt
FROM (
    SELECT
        concat('CREATE DATABASE IF NOT EXISTS `+"`"+`', name, '`+"`"+`', ';') AS stmt,
        1 AS order_type,
        name AS order_name
    FROM system.databases
    WHERE %s
    UNION ALL
    SELECT
        concat(create_table_query, ';') AS stmt,
        multiIf(
            engine = 'View', 4,
            engine = 'MaterializedView', 3,
            2
        ) AS order_type,
        concat(database, '.', name) AS order_name
    FROM system.tables
    WHERE %s
)
ORDER BY order_type, order_name`, dbFilter, tableDbFilter)

	rows, err := e.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute export query: %w", err)
	}
	defer rows.Close()

	result := models.SQLStatements{Version: version}
	for rows.Next() {
		var stmt string
		if err := rows.Scan(&stmt); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		result.Add(stmt)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	// Export SQL UDFs — queried separately to keep the main query simple.
	fnRows, err := e.Query(ctx, "SELECT create_query FROM system.functions WHERE origin = 'SQLUserDefined' ORDER BY name")
	if err == nil {
		defer fnRows.Close()
		for fnRows.Next() {
			var createQuery string
			if err := fnRows.Scan(&createQuery); err != nil {
				return nil, fmt.Errorf("failed to scan function: %w", err)
			}
			result.Add(createQuery + ";")
		}
		if err := fnRows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating functions: %w", err)
		}
	}

	return &result, nil
}

