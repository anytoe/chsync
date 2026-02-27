package models

import "fmt"

// schemaBuilder provides a fluent API for building test Schemas.
// Start with baseSchema() and apply mutations for each test case.
type schemaBuilder struct {
	schema Schema
}

// baseSchema returns a baseline Schema with realistic test data.
// Tests can clone and mutate this to create specific scenarios.
func baseSchema() *schemaBuilder {
	return &schemaBuilder{
		schema: Schema{
			Databases: []Database{
				{
					Name: "db1",
					Tables: []Table{
						{
							Name:    "users",
							Engine:  "MergeTree",
							OrderBy: []string{"id"},
							Columns: []Column{
								{Name: "id", Type: "Int32"},
								{Name: "name", Type: "String"},
							},
						},
						{
							Name:    "orders",
							Engine:  "MergeTree",
							OrderBy: []string{"id"},
							Columns: []Column{
								{Name: "id", Type: "Int32"},
								{Name: "user_id", Type: "Int32"},
								{Name: "amount", Type: "Decimal(10,2)"},
							},
						},
					},
				},
			},
		},
	}
}

// build returns the constructed Schema.
func (b *schemaBuilder) build() Schema {
	return b.schema
}

// clone creates a deep copy of the schema for independent mutations.
func (b *schemaBuilder) clone() *schemaBuilder {
	cloned := &schemaBuilder{
		schema: Schema{
			Databases: make([]Database, len(b.schema.Databases)),
			Functions: append([]Function{}, b.schema.Functions...),
		},
	}
	for i, db := range b.schema.Databases {
		cloned.schema.Databases[i] = Database{
			Name:   db.Name,
			Tables: make([]Table, len(db.Tables)),
		}
		for j, tbl := range db.Tables {
			cloned.schema.Databases[i].Tables[j] = Table{
				Name:        tbl.Name,
				Engine:      tbl.Engine,
				OrderBy:     append([]string{}, tbl.OrderBy...),
				PrimaryKey:  append([]string{}, tbl.PrimaryKey...),
				PartitionBy: tbl.PartitionBy,
				Settings:    make(map[string]string),
				Columns:     make([]Column, len(tbl.Columns)),
			}
			for k, v := range tbl.Settings {
				cloned.schema.Databases[i].Tables[j].Settings[k] = v
			}
			for k, col := range tbl.Columns {
				cloned.schema.Databases[i].Tables[j].Columns[k] = Column{
					Name:              col.Name,
					Type:              col.Type,
					DefaultExpression: col.DefaultExpression,
					CompressionCodec:  col.CompressionCodec,
					Comment:           col.Comment,
				}
			}
		}
	}
	return cloned
}

// addColumn adds a new column to an existing table.
// An optional pos argument inserts the column at that index (0-based).
// Omitting pos appends to the end. Panics if pos exceeds the column count.
func (b *schemaBuilder) addColumn(dbName, tableName, colName, colType string, pos ...int) *schemaBuilder {
	for i := range b.schema.Databases {
		if b.schema.Databases[i].Name != dbName {
			continue
		}
		for j := range b.schema.Databases[i].Tables {
			if b.schema.Databases[i].Tables[j].Name != tableName {
				continue
			}
			cols := b.schema.Databases[i].Tables[j].Columns
			col := Column{Name: colName, Type: colType}
			if len(pos) == 0 {
				b.schema.Databases[i].Tables[j].Columns = append(cols, col)
			} else {
				p := pos[0]
				if p < 0 || p > len(cols) {
					panic(fmt.Sprintf("addColumn: position %d out of range [0, %d]", p, len(cols)))
				}
				cols = append(cols, Column{})
				copy(cols[p+1:], cols[p:])
				cols[p] = col
				b.schema.Databases[i].Tables[j].Columns = cols
			}
			return b
		}
	}
	return b
}

// removeColumn removes a column from a table.
func (b *schemaBuilder) removeColumn(dbName, tableName, colName string) *schemaBuilder {
	for i := range b.schema.Databases {
		if b.schema.Databases[i].Name != dbName {
			continue
		}
		for j := range b.schema.Databases[i].Tables {
			if b.schema.Databases[i].Tables[j].Name != tableName {
				continue
			}
			columns := b.schema.Databases[i].Tables[j].Columns
			for k, col := range columns {
				if col.Name == colName {
					b.schema.Databases[i].Tables[j].Columns = append(columns[:k], columns[k+1:]...)
					return b
				}
			}
		}
	}
	return b
}

// setColumnType changes the type of a column.
func (b *schemaBuilder) setColumnType(dbName, tableName, colName, newType string) *schemaBuilder {
	for i := range b.schema.Databases {
		if b.schema.Databases[i].Name != dbName {
			continue
		}
		for j := range b.schema.Databases[i].Tables {
			if b.schema.Databases[i].Tables[j].Name != tableName {
				continue
			}
			for k := range b.schema.Databases[i].Tables[j].Columns {
				if b.schema.Databases[i].Tables[j].Columns[k].Name == colName {
					b.schema.Databases[i].Tables[j].Columns[k].Type = newType
					return b
				}
			}
		}
	}
	return b
}

// setColumnDefault changes the default expression of a column.
func (b *schemaBuilder) setColumnDefault(dbName, tableName, colName, defaultExpr string) *schemaBuilder {
	for i := range b.schema.Databases {
		if b.schema.Databases[i].Name != dbName {
			continue
		}
		for j := range b.schema.Databases[i].Tables {
			if b.schema.Databases[i].Tables[j].Name != tableName {
				continue
			}
			for k := range b.schema.Databases[i].Tables[j].Columns {
				if b.schema.Databases[i].Tables[j].Columns[k].Name == colName {
					b.schema.Databases[i].Tables[j].Columns[k].DefaultExpression = defaultExpr
					return b
				}
			}
		}
	}
	return b
}

// addTable adds a new table to a database.
func (b *schemaBuilder) addTable(dbName, tableName, engine string, orderBy []string, columns []Column) *schemaBuilder {
	for i := range b.schema.Databases {
		if b.schema.Databases[i].Name == dbName {
			b.schema.Databases[i].Tables = append(
				b.schema.Databases[i].Tables,
				Table{
					Name:     tableName,
					Engine:   engine,
					OrderBy:  orderBy,
					Columns:  columns,
					Settings: make(map[string]string),
				},
			)
			return b
		}
	}
	return b
}

// removeTable removes a table from a database.
func (b *schemaBuilder) removeTable(dbName, tableName string) *schemaBuilder {
	for i := range b.schema.Databases {
		if b.schema.Databases[i].Name != dbName {
			continue
		}
		tables := b.schema.Databases[i].Tables
		for j, table := range tables {
			if table.Name == tableName {
				b.schema.Databases[i].Tables = append(tables[:j], tables[j+1:]...)
				return b
			}
		}
	}
	return b
}

// setTableEngine changes a table's engine.
func (b *schemaBuilder) setTableEngine(dbName, tableName, newEngine string) *schemaBuilder {
	for i := range b.schema.Databases {
		if b.schema.Databases[i].Name != dbName {
			continue
		}
		for j := range b.schema.Databases[i].Tables {
			if b.schema.Databases[i].Tables[j].Name == tableName {
				b.schema.Databases[i].Tables[j].Engine = newEngine
				return b
			}
		}
	}
	return b
}

// setTablePrimaryKey changes a table's PRIMARY KEY clause.
func (b *schemaBuilder) setTablePrimaryKey(dbName, tableName string, primaryKey []string) *schemaBuilder {
	for i := range b.schema.Databases {
		if b.schema.Databases[i].Name != dbName {
			continue
		}
		for j := range b.schema.Databases[i].Tables {
			if b.schema.Databases[i].Tables[j].Name == tableName {
				b.schema.Databases[i].Tables[j].PrimaryKey = primaryKey
				return b
			}
		}
	}
	return b
}

// setTableOrderBy changes a table's ORDER BY clause.
func (b *schemaBuilder) setTableOrderBy(dbName, tableName string, orderBy []string) *schemaBuilder {
	for i := range b.schema.Databases {
		if b.schema.Databases[i].Name != dbName {
			continue
		}
		for j := range b.schema.Databases[i].Tables {
			if b.schema.Databases[i].Tables[j].Name == tableName {
				b.schema.Databases[i].Tables[j].OrderBy = orderBy
				return b
			}
		}
	}
	return b
}

// addDatabase adds a new database.
func (b *schemaBuilder) addDatabase(dbName string) *schemaBuilder {
	b.schema.Databases = append(b.schema.Databases, Database{
		Name:   dbName,
		Tables: []Table{},
	})
	return b
}

// addFunction adds a SQL user-defined function.
func (b *schemaBuilder) addFunction(name, createQuery string) *schemaBuilder {
	b.schema.Functions = append(b.schema.Functions, Function{Name: name, CreateQuery: createQuery})
	return b
}

// removeFunction removes a SQL user-defined function by name.
func (b *schemaBuilder) removeFunction(name string) *schemaBuilder {
	for i, fn := range b.schema.Functions {
		if fn.Name == name {
			b.schema.Functions = append(b.schema.Functions[:i], b.schema.Functions[i+1:]...)
			return b
		}
	}
	return b
}

// removeDatabase removes a database.
func (b *schemaBuilder) removeDatabase(dbName string) *schemaBuilder {
	for i, db := range b.schema.Databases {
		if db.Name == dbName {
			b.schema.Databases = append(b.schema.Databases[:i], b.schema.Databases[i+1:]...)
			return b
		}
	}
	return b
}

// Helper functions for finding elements in schemas

func findColumnInSchema(schema Schema, dbName, tableName, colName string) *Column {
	for _, db := range schema.Databases {
		if db.Name != dbName {
			continue
		}
		for _, table := range db.Tables {
			if table.Name != tableName {
				continue
			}
			for i := range table.Columns {
				if table.Columns[i].Name == colName {
					return &table.Columns[i]
				}
			}
		}
	}
	return nil
}

func findTableInSchema(schema Schema, dbName, tableName string) *Table {
	for _, db := range schema.Databases {
		if db.Name != dbName {
			continue
		}
		for i := range db.Tables {
			if db.Tables[i].Name == tableName {
				return &db.Tables[i]
			}
		}
	}
	return nil
}

func findDatabaseInSchema(schema Schema, dbName string) *Database {
	for i := range schema.Databases {
		if schema.Databases[i].Name == dbName {
			return &schema.Databases[i]
		}
	}
	return nil
}
