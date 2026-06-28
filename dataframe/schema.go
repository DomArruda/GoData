package dataframe

import (
	"database/sql"
	"fmt"
	"strings"
)

// DataType is the coarse logical type of a column. It intentionally mirrors the
// original package's four-way classification so existing callers keep working.
// Internally, DuckDB tracks far richer types; we collapse them into these
// buckets for the Go-facing API.
type DataType int

const (
	TypeInt DataType = iota
	TypeFloat
	TypeString
	TypeTime
)

func (t DataType) String() string {
	switch t {
	case TypeInt:
		return "int"
	case TypeFloat:
		return "float"
	case TypeString:
		return "string"
	case TypeTime:
		return "time"
	default:
		return "unknown"
	}
}

// sqlType returns the DuckDB column type used when we create storage for a
// column of this logical type.
func (t DataType) sqlType() string {
	switch t {
	case TypeInt:
		return "BIGINT"
	case TypeFloat:
		return "DOUBLE"
	case TypeTime:
		return "TIMESTAMP"
	default:
		return "VARCHAR"
	}
}

// dataTypeFromDuckType maps a DuckDB type name (as reported by the driver's
// ColumnTypes or by DESCRIBE) onto one of our four logical buckets.
func dataTypeFromDuckType(name string) DataType {
	n := strings.ToUpper(strings.TrimSpace(name))
	// Strip parameterization like DECIMAL(10,2) down to the base name.
	if i := strings.IndexByte(n, '('); i >= 0 {
		n = n[:i]
	}
	switch n {
	case "TINYINT", "SMALLINT", "INTEGER", "INT", "BIGINT", "HUGEINT",
		"UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT":
		return TypeInt
	case "FLOAT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC":
		return TypeFloat
	case "DATE", "TIME", "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS",
		"TIMESTAMP_NS", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ", "TIMETZ":
		return TypeTime
	default:
		// VARCHAR, BOOLEAN, BLOB, lists, structs, etc. all surface as strings.
		return TypeString
	}
}

// Schema describes the columns of a DataFrame: their names in order, and the
// logical type of each.
type Schema struct {
	Columns  []string
	Types    []DataType
	colIndex map[string]int
}

// Index returns the position of a column by name, and whether it exists.
func (s *Schema) Index(name string) (int, bool) {
	i, ok := s.colIndex[name]
	return i, ok
}

// Has reports whether the schema contains a column with the given name.
func (s *Schema) Has(name string) bool {
	_, ok := s.colIndex[name]
	return ok
}

// TypeOf returns the logical type of a named column.
func (s *Schema) TypeOf(name string) (DataType, bool) {
	i, ok := s.colIndex[name]
	if !ok {
		return 0, false
	}
	return s.Types[i], true
}

// NumericColumns returns the names of columns DuckDB treats as numbers.
func (s *Schema) NumericColumns() []string {
	var out []string
	for i, t := range s.Types {
		if t == TypeInt || t == TypeFloat {
			out = append(out, s.Columns[i])
		}
	}
	return out
}

func buildSchema(cols []string, types []DataType) *Schema {
	idx := make(map[string]int, len(cols))
	for i, c := range cols {
		idx[c] = i
	}
	return &Schema{Columns: cols, Types: types, colIndex: idx}
}

// relationExpr returns the appropriately formatted string for the FROM clause.
// It ensures that subqueries are wrapped in exactly one set of parentheses,
// and base table names (like "df_5") are left unparenthesized to avoid parser errors.
func (df *DataFrame) relationExpr() string {
	trimmed := strings.TrimSpace(df.relation)
	// If it's a raw SELECT query, wrap it in parentheses to make it a valid subquery.
	if strings.HasPrefix(strings.ToUpper(trimmed), "SELECT") {
		return "(" + df.relation + ")"
	}
	// If it's already wrapped in parentheses or it's a base table name, return as is.
	return df.relation
}

// loadSchema asks DuckDB to describe this frame's relation and caches the
// result. It runs the query with LIMIT 0 so no rows are scanned; we only want
// the column metadata.
func (df *DataFrame) loadSchema() (*Schema, error) {
	df.schemaOnce.Do(func() {
		if df.empty {
			df.schema = buildSchema(nil, nil)
			return
		}

		// Use our new helper method instead of manual checking
		fromExpr := df.relationExpr()

		q := "SELECT * FROM " + fromExpr + " AS _df LIMIT 0;"
		rows, err := df.eng.db.Query(q)
		if err != nil {
			df.schemaErr = fmt.Errorf("describing relation: %w", err)
			return
		}
		defer rows.Close()

		colNames, err := rows.Columns()
		if err != nil {
			df.schemaErr = err
			return
		}
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			df.schemaErr = err
			return
		}
		types := make([]DataType, len(colTypes))
		for i, ct := range colTypes {
			types[i] = dataTypeFromDuckType(ct.DatabaseTypeName())
		}
		df.schema = buildSchema(colNames, types)
	})
	return df.schema, df.schemaErr
}

// Schema returns this frame's column metadata, loading it on first use.
func (df *DataFrame) Schema() (*Schema, error) {
	return df.loadSchema()
}

// Columns returns the ordered column names. It panics only if the relation is
// malformed in a way that prevents DuckDB from describing it; for an
// error-returning variant use Schema.
func (df *DataFrame) Columns() ([]string, error) {
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	// Defensive copy so callers can't mutate our cache.
	return append([]string(nil), s.Columns...), nil
}

// scanDest returns a slice of pointers suitable for rows.Scan, one per column,
// typed according to the logical schema. Using sql.Null* wrappers lets us
// faithfully round-trip NULLs.
func scanDest(types []DataType) []interface{} {
	dst := make([]interface{}, len(types))
	for i, t := range types {
		switch t {
		case TypeInt:
			dst[i] = new(sql.NullInt64)
		case TypeFloat:
			dst[i] = new(sql.NullFloat64)
		case TypeTime:
			dst[i] = new(sql.NullTime)
		default:
			dst[i] = new(sql.NullString)
		}
	}
	return dst
}
