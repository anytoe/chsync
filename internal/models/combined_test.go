package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewCombinedSchema_EmptySchemas(t *testing.T) {
	from := Schema{}
	to := Schema{}

	result := NewCombinedSchema(from, to)

	assert.NotNil(t, result)
	assert.Empty(t, result.Databases)
}

func TestNewCombinedSchema_DatabaseOnlyInSource(t *testing.T) {
	from := Schema{
		Databases: []Database{
			{Name: "db1"},
		},
	}
	to := Schema{}

	result := NewCombinedSchema(from, to)

	assert.Len(t, result.Databases, 1)
	assert.Equal(t, "db1", result.Databases[0].Name)
	assert.Equal(t, Source, result.Databases[0].Presence)
}

func TestNewCombinedSchema_DatabaseOnlyInTarget(t *testing.T) {
	from := Schema{}
	to := Schema{
		Databases: []Database{
			{Name: "db1"},
		},
	}

	result := NewCombinedSchema(from, to)

	assert.Len(t, result.Databases, 1)
	assert.Equal(t, "db1", result.Databases[0].Name)
	assert.Equal(t, Target, result.Databases[0].Presence)
}

func TestNewCombinedSchema_DatabaseInBoth(t *testing.T) {
	from := Schema{
		Databases: []Database{
			{Name: "db1"},
		},
	}
	to := Schema{
		Databases: []Database{
			{Name: "db1"},
		},
	}

	result := NewCombinedSchema(from, to)

	assert.Len(t, result.Databases, 1)
	assert.Equal(t, "db1", result.Databases[0].Name)
	assert.Equal(t, Both, result.Databases[0].Presence)
}

func TestNewCombinedSchema_TableOnlyInSource(t *testing.T) {
	from := Schema{
		Databases: []Database{
			{
				Name: "db1",
				Tables: []Table{
					{
						Name:   "table1",
						Engine: "MergeTree",
					},
				},
			},
		},
	}
	to := Schema{
		Databases: []Database{
			{Name: "db1"},
		},
	}

	result := NewCombinedSchema(from, to)

	assert.Len(t, result.Databases, 1)

	db := result.Databases[0]
	assert.Len(t, db.Tables, 1)

	table := db.Tables[0]
	assert.Equal(t, "table1", table.Name)
	assert.Equal(t, Source, table.Presence)
	assert.NotNil(t, table.Source)
	assert.Equal(t, "MergeTree", table.Source.Engine)
	assert.Nil(t, table.Target)
}

func TestNewCombinedSchema_TableOnlyInTarget(t *testing.T) {
	from := Schema{
		Databases: []Database{
			{Name: "db1"},
		},
	}
	to := Schema{
		Databases: []Database{
			{
				Name: "db1",
				Tables: []Table{
					{
						Name:   "table1",
						Engine: "ReplicatedMergeTree",
					},
				},
			},
		},
	}

	result := NewCombinedSchema(from, to)

	db := result.Databases[0]
	assert.Len(t, db.Tables, 1)

	table := db.Tables[0]
	assert.Equal(t, Target, table.Presence)
	assert.Nil(t, table.Source)
	assert.NotNil(t, table.Target)
	assert.Equal(t, "ReplicatedMergeTree", table.Target.Engine)
}

func TestNewCombinedSchema_TableInBoth(t *testing.T) {
	from := Schema{
		Databases: []Database{
			{
				Name: "db1",
				Tables: []Table{
					{
						Name:        "table1",
						Engine:      "MergeTree",
						OrderBy:     []string{"id"},
						PartitionBy: "toYYYYMM(date)",
						Settings:    map[string]string{"index_granularity": "8192"},
					},
				},
			},
		},
	}
	to := Schema{
		Databases: []Database{
			{
				Name: "db1",
				Tables: []Table{
					{
						Name:        "table1",
						Engine:      "ReplicatedMergeTree",
						OrderBy:     []string{"id", "date"},
						PartitionBy: "toYYYYMM(date)",
						Settings:    map[string]string{"index_granularity": "4096"},
					},
				},
			},
		},
	}

	result := NewCombinedSchema(from, to)

	table := result.Databases[0].Tables[0]

	assert.Equal(t, Both, table.Presence)

	assert.NotNil(t, table.Source)
	assert.Equal(t, "MergeTree", table.Source.Engine)
	assert.Equal(t, []string{"id"}, table.Source.OrderBy)
	assert.Equal(t, "8192", table.Source.Settings["index_granularity"])

	assert.NotNil(t, table.Target)
	assert.Equal(t, "ReplicatedMergeTree", table.Target.Engine)
	assert.Equal(t, []string{"id", "date"}, table.Target.OrderBy)
	assert.Equal(t, "4096", table.Target.Settings["index_granularity"])
}

func TestNewCombinedSchema_ColumnOnlyInSource(t *testing.T) {
	from := Schema{
		Databases: []Database{
			{
				Name: "db1",
				Tables: []Table{
					{
						Name: "table1",
						Columns: []Column{
							{
								Name:              "col1",
								Type:              "UInt64",
								DefaultExpression: "0",
								Comment:           "ID column",
							},
						},
					},
				},
			},
		},
	}
	to := Schema{
		Databases: []Database{
			{
				Name:   "db1",
				Tables: []Table{{Name: "table1"}},
			},
		},
	}

	result := NewCombinedSchema(from, to)

	table := result.Databases[0].Tables[0]
	assert.Len(t, table.Columns, 1)

	col := table.Columns[0]
	assert.Equal(t, "col1", col.Name)
	assert.Equal(t, Source, col.Presence)
	assert.NotNil(t, col.Source)
	assert.Equal(t, "UInt64", col.Source.Type)
	assert.Equal(t, "0", col.Source.DefaultExpression)
	assert.Equal(t, "ID column", col.Source.Comment)
	assert.Nil(t, col.Target)
}

func TestNewCombinedSchema_ColumnOnlyInTarget(t *testing.T) {
	from := Schema{
		Databases: []Database{
			{
				Name:   "db1",
				Tables: []Table{{Name: "table1"}},
			},
		},
	}
	to := Schema{
		Databases: []Database{
			{
				Name: "db1",
				Tables: []Table{
					{
						Name: "table1",
						Columns: []Column{
							{
								Name:    "col1",
								Type:    "String",
								Comment: "Name column",
							},
						},
					},
				},
			},
		},
	}

	result := NewCombinedSchema(from, to)

	col := result.Databases[0].Tables[0].Columns[0]

	assert.Equal(t, Target, col.Presence)
	assert.Nil(t, col.Source)
	assert.NotNil(t, col.Target)
	assert.Equal(t, "String", col.Target.Type)
}

func TestNewCombinedSchema_ColumnInBoth(t *testing.T) {
	from := Schema{
		Databases: []Database{
			{
				Name: "db1",
				Tables: []Table{
					{
						Name: "table1",
						Columns: []Column{
							{
								Name:              "col1",
								Type:              "UInt64",
								DefaultExpression: "0",
								Comment:           "ID",
							},
						},
					},
				},
			},
		},
	}
	to := Schema{
		Databases: []Database{
			{
				Name: "db1",
				Tables: []Table{
					{
						Name: "table1",
						Columns: []Column{
							{
								Name:              "col1",
								Type:              "UInt32",
								DefaultExpression: "1",
								Comment:           "Updated ID",
							},
						},
					},
				},
			},
		},
	}

	result := NewCombinedSchema(from, to)

	col := result.Databases[0].Tables[0].Columns[0]

	assert.Equal(t, Both, col.Presence)

	assert.NotNil(t, col.Source)
	assert.Equal(t, "UInt64", col.Source.Type)
	assert.Equal(t, "0", col.Source.DefaultExpression)
	assert.Equal(t, "ID", col.Source.Comment)

	assert.NotNil(t, col.Target)
	assert.Equal(t, "UInt32", col.Target.Type)
	assert.Equal(t, "1", col.Target.DefaultExpression)
	assert.Equal(t, "Updated ID", col.Target.Comment)
}

func TestNewCombinedSchema_ComplexScenario(t *testing.T) {
	from := Schema{
		Databases: []Database{
			{
				Name: "db1",
				Tables: []Table{
					{
						Name:   "table1",
						Engine: "MergeTree",
						Columns: []Column{
							{Name: "col1", Type: "UInt64"},
							{Name: "col2", Type: "String"},
						},
					},
					{
						Name:   "table2",
						Engine: "Log",
					},
				},
			},
			{
				Name: "db2",
				Tables: []Table{
					{Name: "table3"},
				},
			},
		},
	}
	to := Schema{
		Databases: []Database{
			{
				Name: "db1",
				Tables: []Table{
					{
						Name:   "table1",
						Engine: "ReplicatedMergeTree",
						Columns: []Column{
							{Name: "col1", Type: "UInt32"},
							{Name: "col3", Type: "DateTime"},
						},
					},
				},
			},
			{
				Name: "db3",
				Tables: []Table{
					{Name: "table4"},
				},
			},
		},
	}

	result := NewCombinedSchema(from, to)

	// Should have 3 databases: db1 (Both), db2 (Source), db3 (Target)
	assert.Len(t, result.Databases, 3)

	// Find each database
	dbMap := make(map[string]CombinedDatabase)
	for _, db := range result.Databases {
		dbMap[db.Name] = db
	}

	// Check db1 (Both)
	db1, exists := dbMap["db1"]
	assert.True(t, exists, "Expected db1 to exist")
	assert.Equal(t, Both, db1.Presence)
	assert.Len(t, db1.Tables, 2, "db1 should have 2 tables")

	// Check db2 (Source only)
	db2, exists := dbMap["db2"]
	assert.True(t, exists, "Expected db2 to exist")
	assert.Equal(t, Source, db2.Presence)

	// Check db3 (Target only)
	db3, exists := dbMap["db3"]
	assert.True(t, exists, "Expected db3 to exist")
	assert.Equal(t, Target, db3.Presence)

	// Find table1 in db1
	var table1 CombinedTable
	for _, tbl := range db1.Tables {
		if tbl.Name == "table1" {
			table1 = tbl
			break
		}
	}

	// table1 should have 3 columns: col1 (Both), col2 (Source), col3 (Target)
	assert.Len(t, table1.Columns, 3)

	colMap := make(map[string]CombinedColumn)
	for _, col := range table1.Columns {
		colMap[col.Name] = col
	}

	col1, exists := colMap["col1"]
	assert.True(t, exists, "Expected col1 to exist")
	assert.Equal(t, Both, col1.Presence)

	col2, exists := colMap["col2"]
	assert.True(t, exists, "Expected col2 to exist")
	assert.Equal(t, Source, col2.Presence)

	col3, exists := colMap["col3"]
	assert.True(t, exists, "Expected col3 to exist")
	assert.Equal(t, Target, col3.Presence)
}
