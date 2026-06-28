package dataframe

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// This file builds DataFrames from in-memory Go values. File-based sources
// (CSV/JSON/Parquet) live in io.go and lean on DuckDB's native readers; here we
// handle data that already lives in Go structs, maps, and slices.

// materialize creates a real DuckDB table, populates it from the given column
// definitions and row values, and returns a DataFrame over it. Building a base
// table (rather than a view over a VALUES list) means downstream operations are
// cheap and the data is shared by reference across derived frames.
//
// cols/types describe the schema. rows is row-major: each inner slice has one
// element per column, already converted to a type DuckDB's driver accepts
// (int64, float64, string, time.Time, or nil).

func coerceToType(v interface{}, t DataType) interface{} {
	if v == nil {
		return nil
	}

	// NEW: Handle string values coming from Filter("col > 123") etc.
	switch t {
	case TypeInt:
		if s, ok := v.(string); ok {
			if i, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
				return i
			}
			return nil
		}
	case TypeFloat:
		if s, ok := v.(string); ok {
			if f, err := strconv.ParseFloat(strings.TrimSpace(s), 64); err == nil {
				return f
			}
			return nil
		}
	}

	// Existing logic (keep everything below this line unchanged)
	switch t {
	case TypeInt:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return rv.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return int64(rv.Uint())
		case reflect.Float32, reflect.Float64:
			return int64(rv.Float())
		default:
			return nil
		}
	case TypeFloat:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Float32, reflect.Float64:
			return rv.Float()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return float64(rv.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return float64(rv.Uint())
		default:
			return nil
		}
	case TypeTime:
		switch x := v.(type) {
		case time.Time:
			return x
		case string:
			if ts, err := time.Parse(time.RFC3339, x); err == nil {
				return ts
			}
			if ts, err := time.Parse("2006-01-02", x); err == nil {
				return ts
			}
			return nil
		default:
			return nil
		}
	default: // TypeString
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprintf("%v", v)
	}
}

func materialize(e *Engine, cols []string, types []DataType, rows [][]interface{}) (*DataFrame, error) {
	if len(cols) != len(types) {
		return nil, fmt.Errorf("column count (%d) does not match type count (%d)", len(cols), len(types))
	}
	if len(cols) == 0 {
		// DuckDB cannot represent a zero-column table; model it as an empty frame.
		return emptyFrame(e), nil
	}
	name := e.newName("df")

	// CREATE TABLE with an explicit schema so types are exactly what we intend
	// rather than whatever gets inferred from the first row.
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s (", quoteIdent(name))
	for i, c := range cols {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s %s", quoteIdent(c), types[i].sqlType())
	}
	b.WriteString(")")

	if _, err := e.db.Exec(b.String()); err != nil {
		return nil, fmt.Errorf("creating table: %w", err)
	}

	if len(rows) > 0 {
		if err := insertRows(e, name, cols, rows); err != nil {
			return nil, err
		}
	}
	return newOwnedDataFrame(e, name), nil
}

// insertRows bulk-loads rows using a parameterized multi-row INSERT. We chunk
// to keep statements a reasonable size; DuckDB handles large appends well but
// extremely wide VALUES lists are awkward to prepare.
//
// (The DuckDB Appender API is faster still, but it requires reaching through to
// a driver-specific *sql.Conn; a batched INSERT keeps this code portable and is
// more than adequate for the data sizes a Go-side constructor sees.)
func insertRows(e *Engine, table string, cols []string, rows [][]interface{}) error {
	const chunk = 512
	placeholders := func(n int) string {
		ph := make([]string, n)
		for i := range ph {
			ph[i] = "?"
		}
		return "(" + strings.Join(ph, ", ") + ")"
	}
	colList := quoteIdents(cols)

	for start := 0; start < len(rows); start += chunk {
		end := start + chunk
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[start:end]

		var b strings.Builder
		fmt.Fprintf(&b, "INSERT INTO %s (%s) VALUES ", quoteIdent(table), colList)
		args := make([]interface{}, 0, len(batch)*len(cols))
		for i, row := range batch {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(placeholders(len(cols)))
			args = append(args, row...)
		}
		if _, err := e.db.Exec(b.String(), args...); err != nil {
			return fmt.Errorf("inserting rows: %w", err)
		}
	}
	return nil
}

// FromMaps builds a DataFrame from a slice of column->value maps, inferring
// each column's type from the values present. Columns are ordered
// alphabetically for determinism (Go map iteration is random). Missing or nil
// entries become NULL.
//
// Type inference per column: if any value is a string that doesn't parse as a
// timestamp, the column is a string; otherwise time beats float beats int.
func FromMaps(data []map[string]interface{}) (*DataFrame, error) {
	e, err := getDefaultEngine()
	if err != nil {
		return nil, err
	}
	return FromMapsEngine(e, data)
}

// FromMapsEngine is FromMaps against a specific engine.
func FromMapsEngine(e *Engine, data []map[string]interface{}) (*DataFrame, error) {
	if len(data) == 0 {
		return emptyFrame(e), nil
	}

	// Collect the union of keys, sorted for stable column order.
	keySet := map[string]struct{}{}
	for _, row := range data {
		for k := range row {
			keySet[k] = struct{}{}
		}
	}
	cols := make([]string, 0, len(keySet))
	for k := range keySet {
		cols = append(cols, k)
	}
	sort.Strings(cols)

	types := inferTypes(cols, data)
	rows := make([][]interface{}, len(data))
	for i, row := range data {
		out := make([]interface{}, len(cols))
		for j, c := range cols {
			out[j] = coerceToType(row[c], types[j])
		}
		rows[i] = out
	}
	return materialize(e, cols, types, rows)
}

// inferTypes determines a logical type per column from sampled values.
func inferTypes(cols []string, data []map[string]interface{}) []DataType {
	types := make([]DataType, len(cols))
	for j, c := range cols {
		var sawString, sawTime, sawFloat, sawInt bool
		for _, row := range data {
			v, ok := row[c]
			if !ok || v == nil {
				continue
			}
			switch x := v.(type) {
			case int, int8, int16, int32, int64,
				uint, uint8, uint16, uint32, uint64:
				sawInt = true
			case float32, float64:
				sawFloat = true
			case time.Time:
				sawTime = true
			case string:
				if _, err := time.Parse(time.RFC3339, x); err == nil {
					sawTime = true
				} else {
					sawString = true
				}
			default:
				sawString = true
			}
		}
		switch {
		case sawString:
			types[j] = TypeString
		case sawTime:
			types[j] = TypeTime
		case sawFloat:
			types[j] = TypeFloat
		case sawInt:
			types[j] = TypeInt
		default:
			types[j] = TypeString
		}
	}
	return types
}

// FromStructs builds a DataFrame from a slice of structs using reflection.
// Exported fields become columns, in declaration order. Field types map as:
// integer kinds -> TypeInt, float kinds -> TypeFloat, time.Time -> TypeTime,
// everything else -> TypeString (via fmt). A `df:"name"` struct tag overrides
// the column name; `df:"-"` skips the field.
//
// This replaces the original's hard-coded Person special case with something
// that works for any struct.
func FromStructs(data interface{}) (*DataFrame, error) {
	e, err := getDefaultEngine()
	if err != nil {
		return nil, err
	}
	return FromStructsEngine(e, data)
}

// FromStructsEngine is FromStructs against a specific engine.
func FromStructsEngine(e *Engine, data interface{}) (*DataFrame, error) {
	rv := reflect.ValueOf(data)
	if rv.Kind() != reflect.Slice {
		return nil, fmt.Errorf("FromStructs expects a slice, got %T", data)
	}
	elemType := rv.Type().Elem()
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}
	if elemType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("FromStructs expects a slice of structs, got slice of %s", elemType.Kind())
	}

	// Resolve the exported fields once.
	type fieldPlan struct {
		index int
		name  string
		typ   DataType
	}
	var plan []fieldPlan
	for i := 0; i < elemType.NumField(); i++ {
		f := elemType.Field(i)
		if f.PkgPath != "" { // unexported
			continue
		}
		tag := f.Tag.Get("df")
		if tag == "-" {
			continue
		}
		name := f.Name
		if tag != "" {
			name = tag
		}
		plan = append(plan, fieldPlan{index: i, name: name, typ: kindToDataType(f.Type)})
	}

	cols := make([]string, len(plan))
	types := make([]DataType, len(plan))
	for i, p := range plan {
		cols[i] = p.name
		types[i] = p.typ
	}

	rows := make([][]interface{}, rv.Len())
	for r := 0; r < rv.Len(); r++ {
		elem := rv.Index(r)
		if elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		out := make([]interface{}, len(plan))
		for j, p := range plan {
			fv := elem.Field(p.index)
			out[j] = fieldValue(fv, p.typ)
		}
		rows[r] = out
	}
	return materialize(e, cols, types, rows)
}

// kindToDataType maps a struct field's reflect.Type to a logical type.
func kindToDataType(t reflect.Type) DataType {
	if t == reflect.TypeOf(time.Time{}) {
		return TypeTime
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return TypeInt
	case reflect.Float32, reflect.Float64:
		return TypeFloat
	default:
		return TypeString
	}
}

// fieldValue extracts a struct field as a driver-ready value.
func fieldValue(fv reflect.Value, t DataType) interface{} {
	switch t {
	case TypeInt:
		switch fv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return fv.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return int64(fv.Uint())
		}
	case TypeFloat:
		return fv.Float()
	case TypeTime:
		if ts, ok := fv.Interface().(time.Time); ok {
			return ts
		}
	}
	return fmt.Sprintf("%v", fv.Interface())
}
