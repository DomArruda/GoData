package dataframe

import (
	"fmt"
	"strings"
	"time"
)

// Describe returns per-column summary statistics for the numeric columns:
// count, mean, std, min, 25%, 50%, 75%, and max. The output frame has a "stat"
// label column followed by one column per numeric input column, matching the
// shape of the original Describe.
//
// The original computed quantiles in Go after sorting; here DuckDB's
// QUANTILE_CONT does it in-engine.
func (df *DataFrame) Describe() (*DataFrame, error) {
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	numCols := s.NumericColumns()
	if len(numCols) == 0 {
		return nil, fmt.Errorf("Describe: no numeric columns")
	}

	// Each statistic is one row. We compute every stat for every numeric column
	// in a single pass using UNION ALL of one-row SELECTs, then the labels line
	// them up. Using a list keeps the row order deterministic.
	type stat struct {
		label string
		expr  func(col string) string
	}
	stats := []stat{
		{"count", func(c string) string { return "CAST(COUNT(" + c + ") AS DOUBLE)" }},
		{"mean", func(c string) string { return "AVG(" + c + ")" }},
		{"std", func(c string) string { return "STDDEV_SAMP(" + c + ")" }},
		{"min", func(c string) string { return "MIN(" + c + ")" }},
		{"25%", func(c string) string { return "QUANTILE_CONT(" + c + ", 0.25)" }},
		{"50%", func(c string) string { return "QUANTILE_CONT(" + c + ", 0.50)" }},
		{"75%", func(c string) string { return "QUANTILE_CONT(" + c + ", 0.75)" }},
		{"max", func(c string) string { return "MAX(" + c + ")" }},
	}

	selects := make([]string, len(stats))
	for i, st := range stats {
		projs := make([]string, 0, len(numCols)+1)
		projs = append(projs, fmt.Sprintf("%s AS %s", quoteLit(st.label), quoteIdent("stat")))
		for _, c := range numCols {
			projs = append(projs, fmt.Sprintf("%s AS %s", st.expr(quoteIdent(c)), quoteIdent(c)))
		}
		// Each sub-select reads the same source relation.
		selects[i] = fmt.Sprintf("SELECT %s FROM (%s) AS _src",
			strings.Join(projs, ", "), df.relation)
	}

	// Preserve statistic order with an index column. ORDER BY at the top level
	// is the only form SQL guarantees, and DuckDB permits ordering by a column
	// present in the FROM relation even when EXCLUDEd from the projection.
	ordered := make([]string, len(selects))
	for i, sel := range selects {
		ordered[i] = fmt.Sprintf("SELECT %d AS _ord, * FROM %s AS _s", i, sel)
	}
	union := strings.Join(ordered, " UNION ALL ")
	final := fmt.Sprintf("SELECT * EXCLUDE (_ord) FROM %s AS _u ORDER BY _ord", union)

	return newDataFrame(df.eng, final), nil
}

// CumSum returns a frame where each numeric column is replaced by its running
// cumulative sum over the current row order. Non-numeric columns pass through
// unchanged. NULLs are treated as zero for the running total (they don't reset
// it), matching the original's skip-NaN behavior.
//
// Row order matters for a cumulative sum; chain Sort beforehand if you need a
// particular order. We materialize a row number to define "preceding rows"
// deterministically.
func (df *DataFrame) CumSum() (*DataFrame, error) {
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}

	// Establish a stable order via row_number() so the window frame is
	// well-defined even though the source has no inherent ordering.
	withRowNum := fmt.Sprintf(
		"SELECT *, row_number() OVER () AS _rn FROM (%s) AS _src", df.relation)

	projs := make([]string, 0, len(s.Columns))
	for i, c := range s.Columns {
		if s.Types[i] == TypeInt || s.Types[i] == TypeFloat {
			projs = append(projs, fmt.Sprintf(
				"SUM(COALESCE(%s, 0)) OVER (ORDER BY _rn ROWS UNBOUNDED PRECEDING) AS %s",
				quoteIdent(c), quoteIdent(c)))
		} else {
			projs = append(projs, quoteIdent(c))
		}
	}

	q := fmt.Sprintf("SELECT %s FROM %s AS _r ORDER BY _rn",
		strings.Join(projs, ", "), withRowNum)
	return newDataFrame(df.eng, q), nil
}

// AddSeries returns a frame with a new column appended from Go-side data. The
// data length must equal the frame's current row count. Supported element
// types are float64 (numeric column) and time.Time (time column).
//
// Because a DataFrame is a lazy relation with no inherent row order, "append
// this slice positionally" only makes sense against a fixed ordering. We
// therefore materialize the current frame with a row number, build a small
// table from the slice with matching row numbers, and join them. If you care
// which row gets which value, Sort first so the ordering is defined.
func (df *DataFrame) AddSeries(name string, data interface{}) (*DataFrame, error) {
	n, err := df.NumRows()
	if err != nil {
		return nil, err
	}

	var (
		colType DataType
		values  [][]interface{}
	)
	switch v := data.(type) {
	case []float64:
		if len(v) != n {
			return nil, fmt.Errorf("AddSeries: data length (%d) != rows (%d)", len(v), n)
		}
		colType = TypeFloat
		values = make([][]interface{}, n)
		for i, f := range v {
			values[i] = []interface{}{int64(i + 1), f}
		}
	case []time.Time:
		if len(v) != n {
			return nil, fmt.Errorf("AddSeries: data length (%d) != rows (%d)", len(v), n)
		}
		colType = TypeTime
		values = make([][]interface{}, n)
		for i, t := range v {
			values[i] = []interface{}{int64(i + 1), t}
		}
	default:
		return nil, fmt.Errorf("AddSeries: unsupported series type %T", data)
	}

	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	if s.Has(name) {
		return nil, fmt.Errorf("AddSeries: column %q already exists", name)
	}

	// Build an owned side table (_rn, value) for the new series.
	sideName := df.eng.newName("series")
	createSide := fmt.Sprintf("CREATE TABLE %s (_rn BIGINT, %s %s)",
		quoteIdent(sideName), quoteIdent(name), colType.sqlType())
	if _, err := df.eng.db.Exec(createSide); err != nil {
		return nil, fmt.Errorf("AddSeries: creating side table: %w", err)
	}
	if err := insertRows(df.eng, sideName, []string{"_rn", name}, values); err != nil {
		return nil, err
	}

	// Number the base frame's rows, then join the series on row number.
	baseProjs := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		baseProjs[i] = "b." + quoteIdent(c)
	}
	q := fmt.Sprintf(
		"SELECT %s, sv.%s AS %s FROM "+
			"(SELECT *, row_number() OVER () AS _rn FROM (%s) AS _src) AS b "+
			"JOIN %s AS sv ON b._rn = sv._rn "+
			"ORDER BY b._rn",
		strings.Join(baseProjs, ", "),
		quoteIdent(name), quoteIdent(name),
		df.relation,
		quoteIdent(sideName),
	)
	return newDataFrame(df.eng, q), nil
}
