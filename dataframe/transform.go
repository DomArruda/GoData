package dataframe

import (
	"fmt"
	"strings"
)

// Every transformation here returns a NEW DataFrame and never mutates the
// receiver. Each one builds a SQL query that wraps the current relation, so the
// whole chain is lazy: nothing executes until you read results. This is the
// core simplification over the original, which manually shuffled parallel
// numeric/string/time column arrays for each operation.

// Select returns a frame containing only the named columns, in the given order.
// Unknown column names are reported as an error (the original silently dropped
// them, which hid typos).
func (df *DataFrame) Select(columns ...string) (*DataFrame, error) {
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	for _, c := range columns {
		if !s.Has(c) {
			return nil, fmt.Errorf("Select: unknown column %q", c)
		}
	}
	q := fmt.Sprintf("SELECT %s FROM %s", quoteIdents(columns), srcToken)
	return df.derive(q), nil
}

// Drop returns a frame with the named columns removed. Columns that don't exist
// are ignored, matching the intuitive "remove if present" semantics.
func (df *DataFrame) Drop(columns ...string) (*DataFrame, error) {
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	remove := make(map[string]struct{}, len(columns))
	for _, c := range columns {
		remove[c] = struct{}{}
	}
	var keep []string
	for _, c := range s.Columns {
		if _, drop := remove[c]; !drop {
			keep = append(keep, c)
		}
	}
	if len(keep) == 0 {
		return nil, fmt.Errorf("Drop: cannot drop all columns")
	}
	q := fmt.Sprintf("SELECT %s FROM %s", quoteIdents(keep), srcToken)
	return df.derive(q), nil
}

// Rename returns a frame with one column renamed. Other columns pass through
// unchanged and in order.
func (df *DataFrame) Rename(oldName, newName string) (*DataFrame, error) {
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	if !s.Has(oldName) {
		return nil, fmt.Errorf("Rename: column %q not found", oldName)
	}
	projs := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		if c == oldName {
			projs[i] = fmt.Sprintf("%s AS %s", quoteIdent(c), quoteIdent(newName))
		} else {
			projs[i] = quoteIdent(c)
		}
	}
	q := fmt.Sprintf("SELECT %s FROM %s", strings.Join(projs, ", "), srcToken)
	return df.derive(q), nil
}

// Head returns the first n rows. Note: without an explicit Sort, "first" is
// whatever order the engine produces; chain Sort first if order matters.
func (df *DataFrame) Head(n int) *DataFrame {
	if n < 0 {
		n = 0
	}
	q := fmt.Sprintf("SELECT * FROM %s LIMIT %d", srcToken, n)
	return df.derive(q)
}

// Tail returns the last n rows by skipping the first (total-n) rows with
// OFFSET. Like Head, "last" is defined by whatever order the underlying
// relation produces; for base tables that's insertion order, but if a
// particular order matters, call Sort first.
func (df *DataFrame) Tail(n int) (*DataFrame, error) {
	if n < 0 {
		n = 0
	}
	total, err := df.NumRows()
	if err != nil {
		return nil, err
	}
	offset := total - n
	if offset < 0 {
		offset = 0
	}
	q := fmt.Sprintf("SELECT * FROM %s OFFSET %d", srcToken, offset)
	return df.derive(q), nil
}

// Limit returns at most n rows starting at the given offset. It's the general
// form behind Head/Tail and is handy for pagination.
func (df *DataFrame) Limit(offset, n int) *DataFrame {
	if offset < 0 {
		offset = 0
	}
	if n < 0 {
		n = 0
	}
	q := fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", srcToken, n, offset)
	return df.derive(q)
}

// Distinct returns a frame with duplicate rows removed. With no columns, whole
// rows must match; with columns, DuckDB's DISTINCT ON keeps the first row per
// distinct combination of those columns.
func (df *DataFrame) Distinct(columns ...string) (*DataFrame, error) {
	if len(columns) == 0 {
		q := fmt.Sprintf("SELECT DISTINCT * FROM %s", srcToken)
		return df.derive(q), nil
	}
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	for _, c := range columns {
		if !s.Has(c) {
			return nil, fmt.Errorf("Distinct: unknown column %q", c)
		}
	}
	q := fmt.Sprintf("SELECT DISTINCT ON (%s) * FROM %s",
		quoteIdents(columns), srcToken)
	return df.derive(q), nil
}

// DropNA returns a frame with rows containing any NULL removed. If columns are
// given, only those are checked; otherwise every column is checked.
func (df *DataFrame) DropNA(columns ...string) (*DataFrame, error) {
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	check := columns
	if len(check) == 0 {
		check = s.Columns
	}
	for _, c := range check {
		if !s.Has(c) {
			return nil, fmt.Errorf("DropNA: unknown column %q", c)
		}
	}
	conds := make([]string, len(check))
	for i, c := range check {
		conds[i] = quoteIdent(c) + " IS NOT NULL"
	}
	q := fmt.Sprintf("SELECT * FROM %s WHERE %s",
		srcToken, strings.Join(conds, " AND "))
	return df.derive(q), nil
}

// FillNA returns a frame with NULLs replaced per column. The map keys are
// column names; values are the fill value for that column, coerced to the
// column's type. Columns not in the map are unchanged.
func (df *DataFrame) FillNA(values map[string]interface{}) (*DataFrame, error) {
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	projs := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		if fill, ok := values[c]; ok {
			t, _ := s.TypeOf(c)
			lit := quoteLit(coerceLit(fill, t))
			projs[i] = fmt.Sprintf("COALESCE(%s, %s) AS %s",
				quoteIdent(c), lit, quoteIdent(c))
		} else {
			projs[i] = quoteIdent(c)
		}
	}
	q := fmt.Sprintf("SELECT %s FROM %s", strings.Join(projs, ", "), srcToken)
	return df.derive(q), nil
}

// coerceLit normalizes a fill value to a form quoteLit renders correctly for
// the target column type (e.g. parse string dates into time.Time).
func coerceLit(v interface{}, t DataType) interface{} {
	c := coerceToType(v, t)
	if c == nil {
		return v // let quoteLit do its best rather than emit NULL
	}
	return c
}

// AsType returns a frame with one column cast to a new logical type using
// DuckDB's CAST. Casts that DuckDB can't perform surface as a query error when
// the frame is read. Unlike the original, this works between any of the four
// types DuckDB supports converting.
func (df *DataFrame) AsType(column string, newType DataType) (*DataFrame, error) {
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	if !s.Has(column) {
		return nil, fmt.Errorf("AsType: column %q not found", column)
	}
	projs := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		if c == column {
			// TRY_CAST yields NULL instead of erroring on bad values, matching
			// the original's lenient "unparseable becomes NaN/null" behavior.
			projs[i] = fmt.Sprintf("TRY_CAST(%s AS %s) AS %s",
				quoteIdent(c), newType.sqlType(), quoteIdent(c))
		} else {
			projs[i] = quoteIdent(c)
		}
	}
	q := fmt.Sprintf("SELECT %s FROM %s", strings.Join(projs, ", "), srcToken)
	return df.derive(q), nil
}

// WithColumn returns a frame with an added or replaced column defined by a raw
// SQL expression evaluated over the existing columns. For example:
//
//	df.WithColumn("total", "price * quantity")
//
// The expression is trusted SQL (the API surface for computed columns); column
// references inside it should be quoted by the caller if they contain unusual
// characters. This is the escape hatch that lets callers express computations
// the typed helpers don't cover.
func (df *DataFrame) WithColumn(name, sqlExpr string) (*DataFrame, error) {
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	var projs []string
	replaced := false
	for _, c := range s.Columns {
		if c == name {
			projs = append(projs, fmt.Sprintf("(%s) AS %s", sqlExpr, quoteIdent(name)))
			replaced = true
		} else {
			projs = append(projs, quoteIdent(c))
		}
	}
	if !replaced {
		projs = append(projs, fmt.Sprintf("(%s) AS %s", sqlExpr, quoteIdent(name)))
	}
	q := fmt.Sprintf("SELECT %s FROM %s", strings.Join(projs, ", "), srcToken)
	return df.derive(q), nil
}
