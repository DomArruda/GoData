package dataframe

import (
	"fmt"
	"strings"
)

// Sort returns a frame ordered by the given columns. ascending[i] controls the
// direction of columns[i]; if ascending is shorter than columns, missing
// entries default to ascending. NULLs sort last in ascending order (DuckDB's
// default), which matches typical expectations.
func (df *DataFrame) Sort(columns []string, ascending []bool) (*DataFrame, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("Sort: no columns given")
	}
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	terms := make([]string, len(columns))
	for i, c := range columns {
		if !s.Has(c) {
			return nil, fmt.Errorf("Sort: unknown column %q", c)
		}
		dir := "ASC"
		if i < len(ascending) && !ascending[i] {
			dir = "DESC"
		}
		terms[i] = quoteIdent(c) + " " + dir
	}
	q := fmt.Sprintf("SELECT * FROM %s ORDER BY %s",
		srcToken, strings.Join(terms, ", "))
	return df.derive(q), nil
}

// Aggregation pairs a source column with a function to apply to it in GroupBy.
// Func is one of: "sum", "avg"/"mean", "min", "max", "count", "count_distinct",
// "std"/"stddev", "var"/"variance", "median", "first", "last". The result
// column is named "<column>_<func>" unless As is set.
type Aggregation struct {
	Column string
	Func   string
	As     string // optional output column name
}

// aggExpr renders an aggregation as a SQL expression with its output alias.
func (a Aggregation) aggExpr() (string, error) {
	col := quoteIdent(a.Column)
	var fn string
	switch strings.ToLower(a.Func) {
	case "sum":
		fn = "SUM(" + col + ")"
	case "avg", "mean":
		fn = "AVG(" + col + ")"
	case "min":
		fn = "MIN(" + col + ")"
	case "max":
		fn = "MAX(" + col + ")"
	case "count":
		fn = "COUNT(" + col + ")"
	case "count_distinct", "countdistinct", "nunique":
		fn = "COUNT(DISTINCT " + col + ")"
	case "std", "stddev":
		fn = "STDDEV_SAMP(" + col + ")"
	case "var", "variance":
		fn = "VAR_SAMP(" + col + ")"
	case "median":
		fn = "MEDIAN(" + col + ")"
	case "first":
		fn = "FIRST(" + col + ")"
	case "last":
		fn = "LAST(" + col + ")"
	default:
		return "", fmt.Errorf("GroupBy: unsupported aggregation %q", a.Func)
	}
	name := a.As
	if name == "" {
		name = a.Column + "_" + strings.ToLower(a.Func)
	}
	return fmt.Sprintf("%s AS %s", fn, quoteIdent(name)), nil
}

// GroupBy groups by the given columns and computes the given aggregations.
// Output columns are the group keys followed by each aggregation. This replaces
// the original's hand-built grouping with DuckDB's GROUP BY, which is both
// correct for all type combinations and dramatically faster.
//
//	df.GroupBy([]string{"city"},
//	    Aggregation{Column: "sales", Func: "sum"},
//	    Aggregation{Column: "sales", Func: "avg"},
//	)
func (df *DataFrame) GroupBy(groupColumns []string, aggs ...Aggregation) (*DataFrame, error) {
	if len(groupColumns) == 0 {
		return nil, fmt.Errorf("GroupBy: no group columns given")
	}
	if len(aggs) == 0 {
		return nil, fmt.Errorf("GroupBy: no aggregations given")
	}
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	for _, c := range groupColumns {
		if !s.Has(c) {
			return nil, fmt.Errorf("GroupBy: unknown group column %q", c)
		}
	}

	projs := make([]string, 0, len(groupColumns)+len(aggs))
	for _, c := range groupColumns {
		projs = append(projs, quoteIdent(c))
	}
	for _, a := range aggs {
		if !s.Has(a.Column) {
			return nil, fmt.Errorf("GroupBy: unknown aggregation column %q", a.Column)
		}
		expr, err := a.aggExpr()
		if err != nil {
			return nil, err
		}
		projs = append(projs, expr)
	}

	q := fmt.Sprintf("SELECT %s FROM %s GROUP BY %s ORDER BY %s",
		strings.Join(projs, ", "),
		srcToken,
		quoteIdents(groupColumns),
		quoteIdents(groupColumns), // stable, deterministic output order
	)
	return df.derive(q), nil
}

// Agg computes aggregations over the whole frame (no grouping), returning a
// one-row frame. Convenient for summary numbers without a group key.
func (df *DataFrame) Agg(aggs ...Aggregation) (*DataFrame, error) {
	if len(aggs) == 0 {
		return nil, fmt.Errorf("Agg: no aggregations given")
	}
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	projs := make([]string, len(aggs))
	for i, a := range aggs {
		if !s.Has(a.Column) {
			return nil, fmt.Errorf("Agg: unknown column %q", a.Column)
		}
		expr, err := a.aggExpr()
		if err != nil {
			return nil, err
		}
		projs[i] = expr
	}
	q := fmt.Sprintf("SELECT %s FROM %s", strings.Join(projs, ", "), srcToken)
	return df.derive(q), nil
}
