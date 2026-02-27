package models

import (
	"testing"
)

// Tests for schemaBuilder
func TestSchemaBuilder_AddColumn(t *testing.T) {
	schema := baseSchema().
		addColumn("db1", "users", "email", "String").
		build()

	col := findColumnInSchema(schema, "db1", "users", "email")
	if col == nil {
		t.Fatal("email column not found")
	}
	if col.Type != "String" {
		t.Errorf("expected Type=String, got %v", col.Type)
	}
}

func TestSchemaBuilder_RemoveColumn(t *testing.T) {
	schema := baseSchema().
		removeColumn("db1", "users", "name").
		build()

	col := findColumnInSchema(schema, "db1", "users", "name")
	if col != nil {
		t.Error("expected name column to be removed")
	}
}

func TestSchemaBuilder_SetColumnType(t *testing.T) {
	schema := baseSchema().
		setColumnType("db1", "users", "id", "Int64").
		build()

	col := findColumnInSchema(schema, "db1", "users", "id")
	if col == nil {
		t.Fatal("id column not found")
	}
	if col.Type != "Int64" {
		t.Errorf("expected Type=Int64, got %v", col.Type)
	}
}

func TestSchemaBuilder_AddTable(t *testing.T) {
	schema := baseSchema().
		addTable("db1", "products", "MergeTree", []string{"id"}, []Column{
			{Name: "id", Type: "Int32"},
			{Name: "title", Type: "String"},
		}).
		build()

	table := findTableInSchema(schema, "db1", "products")
	if table == nil {
		t.Fatal("products table not found")
	}
	if len(table.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(table.Columns))
	}
}

func TestSchemaBuilder_RemoveTable(t *testing.T) {
	schema := baseSchema().
		removeTable("db1", "orders").
		build()

	table := findTableInSchema(schema, "db1", "orders")
	if table != nil {
		t.Error("expected orders table to be removed")
	}
}

func TestSchemaBuilder_SetTableEngine(t *testing.T) {
	schema := baseSchema().
		setTableEngine("db1", "users", "ReplicatedMergeTree").
		build()

	table := findTableInSchema(schema, "db1", "users")
	if table == nil {
		t.Fatal("users table not found")
	}
	if table.Engine != "ReplicatedMergeTree" {
		t.Errorf("expected Engine=ReplicatedMergeTree, got %v", table.Engine)
	}
}

func TestSchemaBuilder_Clone(t *testing.T) {
	base := baseSchema()
	schema1 := base.clone().addColumn("db1", "users", "email", "String").build()
	schema2 := base.clone().removeColumn("db1", "users", "name").build()

	// schema1 should have email
	if findColumnInSchema(schema1, "db1", "users", "email") == nil {
		t.Error("schema1 should have email column")
	}

	// schema2 should not have name
	if findColumnInSchema(schema2, "db1", "users", "name") != nil {
		t.Error("schema2 should not have name column")
	}

	// schema2 should not have email (independent mutations)
	if findColumnInSchema(schema2, "db1", "users", "email") != nil {
		t.Error("schema2 should not have email column")
	}
}

func TestSchemaBuilder_ChainedOperations(t *testing.T) {
	schema := baseSchema().
		addColumn("db1", "users", "email", "String").
		setColumnType("db1", "users", "id", "Int64").
		removeTable("db1", "orders").
		build()

	// Check email was added
	email := findColumnInSchema(schema, "db1", "users", "email")
	if email == nil || email.Type != "String" {
		t.Error("email column not added correctly")
	}

	// Check id type changed
	id := findColumnInSchema(schema, "db1", "users", "id")
	if id == nil || id.Type != "Int64" {
		t.Error("id type not changed correctly")
	}

	// Check orders removed
	orders := findTableInSchema(schema, "db1", "orders")
	if orders != nil {
		t.Error("orders table not removed")
	}
}

func TestSchemaBuilder_AddColumnAtPosition(t *testing.T) {
	// baseSchema users table has columns: [id, name]

	t.Run("position 0 inserts at front", func(t *testing.T) {
		schema := baseSchema().addColumn("db1", "users", "email", "String", 0).build()
		table := findTableInSchema(schema, "db1", "users")
		if table == nil {
			t.Fatal("users table not found")
		}
		if table.Columns[0].Name != "email" {
			t.Errorf("expected email at index 0, got %s", table.Columns[0].Name)
		}
		if table.Columns[1].Name != "id" {
			t.Errorf("expected id at index 1, got %s", table.Columns[1].Name)
		}
	})

	t.Run("position 1 inserts in middle", func(t *testing.T) {
		schema := baseSchema().addColumn("db1", "users", "email", "String", 1).build()
		table := findTableInSchema(schema, "db1", "users")
		if table == nil {
			t.Fatal("users table not found")
		}
		if table.Columns[1].Name != "email" {
			t.Errorf("expected email at index 1, got %s", table.Columns[1].Name)
		}
		if table.Columns[2].Name != "name" {
			t.Errorf("expected name at index 2, got %s", table.Columns[2].Name)
		}
	})

	t.Run("position equal to len appends to end", func(t *testing.T) {
		schema := baseSchema().addColumn("db1", "users", "email", "String", 2).build()
		table := findTableInSchema(schema, "db1", "users")
		if table == nil {
			t.Fatal("users table not found")
		}
		last := table.Columns[len(table.Columns)-1]
		if last.Name != "email" {
			t.Errorf("expected email at end, got %s", last.Name)
		}
	})

	t.Run("position out of range panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for out-of-range position")
			}
		}()
		baseSchema().addColumn("db1", "users", "email", "String", 99)
	})
}
