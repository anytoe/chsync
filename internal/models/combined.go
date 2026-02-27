package models

// Presence indicates where an element exists
type Presence string

const (
	Source Presence = "Source" // exists only in source instance
	Target Presence = "Target" // exists only in target instance
	Both   Presence = "Both"   // exists in both instances
)

// CombinedSchema represents a merged view of two schemas
type CombinedSchema struct {
	Databases []CombinedDatabase
	Functions []CombinedFunction
}

// CombinedFunction represents a SQL UDF that may exist in source, target, or both
type CombinedFunction struct {
	Name     string
	Presence Presence
	Source   *FunctionProperties
	Target   *FunctionProperties
}

// FunctionProperties contains properties of a SQL UDF
type FunctionProperties struct {
	CreateQuery string
}

// CombinedDatabase represents a database that may exist in source, target, or both
type CombinedDatabase struct {
	Name     string
	Presence Presence
	Tables   []CombinedTable
}

// CombinedTable represents a table that may exist in source, target, or both
type CombinedTable struct {
	Name     string
	Presence Presence
	Source   *TableProperties
	Target   *TableProperties
	Columns  []CombinedColumn
}

// TableProperties contains properties of a table
type TableProperties struct {
	Engine      string
	OrderBy     []string
	PrimaryKey  []string
	PartitionBy string
	Settings    map[string]string
}

// CombinedColumn represents a column that may exist in source, target, or both
type CombinedColumn struct {
	Name     string
	Presence Presence
	Source   *ColumnProperties
	Target   *ColumnProperties
}

// ColumnProperties contains properties of a column
type ColumnProperties struct {
	Type              string
	Position          uint64
	DefaultKind       string
	DefaultExpression string
	CompressionCodec  string
	Comment           string
}

// NewCombinedSchema creates a CombinedSchema by comparing from (source) and to (target) schemas
func NewCombinedSchema(from, to Schema) *CombinedSchema {
	cs := &CombinedSchema{}

	// Create maps for quick lookups
	fromDBMap := make(map[string]Database)
	toDBMap := make(map[string]Database)

	for _, db := range from.Databases {
		fromDBMap[db.Name] = db
	}
	for _, db := range to.Databases {
		toDBMap[db.Name] = db
	}

	// Get all unique database names
	dbNames := make(map[string]bool)
	for name := range fromDBMap {
		dbNames[name] = true
	}
	for name := range toDBMap {
		dbNames[name] = true
	}

	// Process each database
	for dbName := range dbNames {
		fromDB, inFrom := fromDBMap[dbName]
		toDB, inTo := toDBMap[dbName]

		cdb := CombinedDatabase{
			Name: dbName,
		}

		switch {
		case inFrom && inTo:
			cdb.Presence = Both
			cdb.Tables = compareTables(fromDB.Tables, toDB.Tables)
		case inFrom:
			cdb.Presence = Source
			cdb.Tables = tablesFromSource(fromDB.Tables)
		case inTo:
			cdb.Presence = Target
			cdb.Tables = tablesFromTarget(toDB.Tables)
		}

		cs.Databases = append(cs.Databases, cdb)
	}

	cs.Functions = compareFunctions(from.Functions, to.Functions)

	return cs
}

func compareFunctions(fromFuncs, toFuncs []Function) []CombinedFunction {
	fromMap := make(map[string]Function)
	for _, f := range fromFuncs {
		fromMap[f.Name] = f
	}

	inTo := make(map[string]bool)
	var result []CombinedFunction

	for _, toFunc := range toFuncs {
		inTo[toFunc.Name] = true
		cf := CombinedFunction{Name: toFunc.Name}
		if fromFunc, inFrom := fromMap[toFunc.Name]; inFrom {
			cf.Presence = Both
			cf.Source = &FunctionProperties{CreateQuery: fromFunc.CreateQuery}
			cf.Target = &FunctionProperties{CreateQuery: toFunc.CreateQuery}
		} else {
			cf.Presence = Target
			cf.Target = &FunctionProperties{CreateQuery: toFunc.CreateQuery}
		}
		result = append(result, cf)
	}

	for _, fromFunc := range fromFuncs {
		if !inTo[fromFunc.Name] {
			result = append(result, CombinedFunction{
				Name:     fromFunc.Name,
				Presence: Source,
				Source:   &FunctionProperties{CreateQuery: fromFunc.CreateQuery},
			})
		}
	}

	return result
}

func compareTables(fromTables, toTables []Table) []CombinedTable {
	fromMap := make(map[string]Table)
	toMap := make(map[string]Table)

	for _, t := range fromTables {
		fromMap[t.Name] = t
	}
	for _, t := range toTables {
		toMap[t.Name] = t
	}

	tableNames := make(map[string]bool)
	for name := range fromMap {
		tableNames[name] = true
	}
	for name := range toMap {
		tableNames[name] = true
	}

	var result []CombinedTable
	for tableName := range tableNames {
		fromTable, inFrom := fromMap[tableName]
		toTable, inTo := toMap[tableName]

		ct := CombinedTable{
			Name: tableName,
		}

		switch {
		case inFrom && inTo:
			ct.Presence = Both
			ct.Source = tableToProperties(fromTable)
			ct.Target = tableToProperties(toTable)
			ct.Columns = compareColumns(fromTable.Columns, toTable.Columns)
		case inFrom:
			ct.Presence = Source
			ct.Source = tableToProperties(fromTable)
			ct.Columns = columnsFromSource(fromTable.Columns)
		case inTo:
			ct.Presence = Target
			ct.Target = tableToProperties(toTable)
			ct.Columns = columnsFromTarget(toTable.Columns)
		}

		result = append(result, ct)
	}

	return result
}

func tablesFromSource(tables []Table) []CombinedTable {
	var result []CombinedTable
	for _, t := range tables {
		result = append(result, CombinedTable{
			Name:     t.Name,
			Presence: Source,
			Source:   tableToProperties(t),
			Columns:  columnsFromSource(t.Columns),
		})
	}
	return result
}

func tablesFromTarget(tables []Table) []CombinedTable {
	var result []CombinedTable
	for _, t := range tables {
		result = append(result, CombinedTable{
			Name:     t.Name,
			Presence: Target,
			Target:   tableToProperties(t),
			Columns:  columnsFromTarget(t.Columns),
		})
	}
	return result
}

func tableToProperties(t Table) *TableProperties {
	return &TableProperties{
		Engine:      t.Engine,
		OrderBy:     t.OrderBy,
		PrimaryKey:  t.PrimaryKey,
		PartitionBy: t.PartitionBy,
		Settings:    t.Settings,
	}
}

func compareColumns(fromCols, toCols []Column) []CombinedColumn {
	fromMap := make(map[string]Column)
	fromPosMap := make(map[string]uint64)
	for i, c := range fromCols {
		fromMap[c.Name] = c
		fromPosMap[c.Name] = uint64(i)
	}

	inTo := make(map[string]bool)
	var result []CombinedColumn

	// Iterate toCols in order to preserve target column ordering.
	// Record source and target ordinal positions so the sync plan can emit FIRST/AFTER clauses.
	for targetIdx, toCol := range toCols {
		inTo[toCol.Name] = true
		cc := CombinedColumn{Name: toCol.Name}
		if fromCol, inFrom := fromMap[toCol.Name]; inFrom {
			cc.Presence = Both
			cc.Source = columnToProperties(fromCol)
			cc.Source.Position = fromPosMap[toCol.Name]
			cc.Target = columnToProperties(toCol)
			cc.Target.Position = uint64(targetIdx)
		} else {
			cc.Presence = Target
			cc.Target = columnToProperties(toCol)
			cc.Target.Position = uint64(targetIdx)
		}
		result = append(result, cc)
	}

	// Append source-only columns (not in target)
	for srcIdx, fromCol := range fromCols {
		if !inTo[fromCol.Name] {
			props := columnToProperties(fromCol)
			props.Position = uint64(srcIdx)
			result = append(result, CombinedColumn{
				Name:     fromCol.Name,
				Presence: Source,
				Source:   props,
			})
		}
	}

	return result
}

func columnsFromSource(cols []Column) []CombinedColumn {
	var result []CombinedColumn
	for _, c := range cols {
		result = append(result, CombinedColumn{
			Name:     c.Name,
			Presence: Source,
			Source:   columnToProperties(c),
		})
	}
	return result
}

func columnsFromTarget(cols []Column) []CombinedColumn {
	var result []CombinedColumn
	for _, c := range cols {
		result = append(result, CombinedColumn{
			Name:     c.Name,
			Presence: Target,
			Target:   columnToProperties(c),
		})
	}
	return result
}

func columnToProperties(c Column) *ColumnProperties {
	return &ColumnProperties{
		Type:              c.Type,
		DefaultExpression: c.DefaultExpression,
		CompressionCodec:  c.CompressionCodec,
		Comment:           c.Comment,
	}
}
