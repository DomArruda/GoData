package dataframe

import (
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"
)

// This file is where data crosses back from DuckDB into Go. Everything above is
// lazy query-building; the methods here actually execute.

// Row is one materialized record: column name -> value. Numeric columns come
// back as int64 or float64, time columns as time.Time, string columns as
// string, and SQL NULLs as nil.
type Row map[string]interface{}

// NumRows returns the number of rows the frame currently represents by running
// a COUNT(*). It does not materialize the rows themselves.
func (df *DataFrame) NumRows() (int, error) {
	if df.empty {
		return 0, nil
	}
	var n int

	// FIXED: Removed the hardcoded () and use relationExpr() instead
	q := "SELECT COUNT(*) FROM " + df.relationExpr() + " AS _df"

	if err := df.eng.db.QueryRow(q).Scan(&n); err != nil {
		return 0, fmt.Errorf("counting rows: %w", err)
	}
	return n, nil
}


// NumCols returns the number of columns.
func (df *DataFrame) NumCols() (int, error) {
	s, err := df.loadSchema()
	if err != nil {
		return 0, err
	}
	return len(s.Columns), nil
}

// Shape returns (rows, cols), mirroring the original's Rows/Cols fields.
func (df *DataFrame) Shape() (rows, cols int, err error) {
	rows, err = df.NumRows()
	if err != nil {
		return 0, 0, err
	}
	cols, err = df.NumCols()
	if err != nil {
		return 0, 0, err
	}
	return rows, cols, nil
}

// Collect executes the frame's query and returns every row as a []Row. For
// large results prefer Iterate to avoid holding the whole set in memory.
func (df *DataFrame) Collect() ([]Row, error) {
	if df.empty {
		return nil, nil
	}
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	q := "SELECT * FROM (" + df.relation + ") AS _df"
	rows, err := df.eng.db.Query(q)
	if err != nil {
		return nil, fmt.Errorf("collecting rows: %w", err)
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		dst := scanDest(s.Types)
		if err := rows.Scan(dst...); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		out = append(out, rowFromDest(s.Columns, s.Types, dst))
	}
	return out, rows.Err()
}

// Iterate streams rows to a callback, one at a time, stopping early if the
// callback returns false or an error. This keeps memory flat for large frames.
func (df *DataFrame) Iterate(fn func(Row) (bool, error)) error {
	if df.empty {
		return nil
	}
	s, err := df.loadSchema()
	if err != nil {
		return err
	}
	q := "SELECT * FROM " + df.relation + " AS _df"
	rows, err := df.eng.db.Query(q)
	if err != nil {
		return fmt.Errorf("iterating rows: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		dst := scanDest(s.Types)
		if err := rows.Scan(dst...); err != nil {
			return fmt.Errorf("scanning row: %w", err)
		}
		cont, err := fn(rowFromDest(s.Columns, s.Types, dst))
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return rows.Err()
}

// rowFromDest converts a scanned destination slice into a Row, unwrapping the
// sql.Null* wrappers into plain Go values (or nil for NULL).
func rowFromDest(cols []string, types []DataType, dst []interface{}) Row {
	row := make(Row, len(cols))
	for i, c := range cols {
		switch types[i] {
		case TypeInt:
			v := dst[i].(*sql.NullInt64)
			if v.Valid {
				row[c] = v.Int64
			} else {
				row[c] = nil
			}
		case TypeFloat:
			v := dst[i].(*sql.NullFloat64)
			if v.Valid {
				row[c] = v.Float64
			} else {
				row[c] = nil
			}
		case TypeTime:
			v := dst[i].(*sql.NullTime)
			if v.Valid {
				row[c] = v.Time
			} else {
				row[c] = nil
			}
		default:
			v := dst[i].(*sql.NullString)
			if v.Valid {
				row[c] = v.String
			} else {
				row[c] = nil
			}
		}
	}
	return row
}

// Float returns a single numeric scalar from a one-row, one-column frame, e.g.
// the result of Agg(...) with a single aggregation. It's a convenience over
// Collect for "give me the number" cases. If the frame has more than one column
// or more than one row, only the first cell is read.
func (df *DataFrame) Float() (float64, error) {
	s, err := df.loadSchema()
	if err != nil {
		return 0, err
	}
	if len(s.Columns) == 0 {
		return 0, fmt.Errorf("Float: frame has no columns")
	}
	// Read the first column regardless of how many there are, and accept either
	// an integer or floating return from DuckDB.
	q := fmt.Sprintf("SELECT %s FROM (%s) AS _df LIMIT 1",
		quoteIdent(s.Columns[0]), df.relation)
	var (
		fv sql.NullFloat64
		iv sql.NullInt64
	)
	switch s.Types[0] {
	case TypeInt:
		if err := df.eng.db.QueryRow(q).Scan(&iv); err != nil {
			return 0, fmt.Errorf("reading scalar: %w", err)
		}
		if !iv.Valid {
			return math.NaN(), nil
		}
		return float64(iv.Int64), nil
	default:
		if err := df.eng.db.QueryRow(q).Scan(&fv); err != nil {
			return 0, fmt.Errorf("reading scalar: %w", err)
		}
		if !fv.Valid {
			return math.NaN(), nil
		}
		return fv.Float64, nil
	}
}

// Print writes a human-readable table preview to stdout (up to maxRows rows).
// It mirrors the original's Print but reads through the query engine. A maxRows
// of 0 or less defaults to 10.
func (df *DataFrame) Print(maxRows ...int) error {
	limit := 10
	if len(maxRows) > 0 && maxRows[0] > 0 {
		limit = maxRows[0]
	}

	if df.empty {
		fmt.Println("Empty DataFrame")
		return nil
	}

	s, err := df.loadSchema()
	if err != nil {
		return err
	}
	total, err := df.NumRows()
	if err != nil {
		return err
	}
	if total == 0 || len(s.Columns) == 0 {
		fmt.Println("Empty DataFrame")
		return nil
	}

	preview, err := df.Head(limit).Collect()
	if err != nil {
		return err
	}

	// Compute column widths from headers and previewed cells.
	widths := make([]int, len(s.Columns))
	for i, c := range s.Columns {
		widths[i] = len(c)
	}
	cells := make([][]string, len(preview))
	for r, row := range preview {
		cells[r] = make([]string, len(s.Columns))
		for i, c := range s.Columns {
			str := formatCell(row[c], s.Types[i])
			cells[r][i] = str
			if len(str) > widths[i] {
				widths[i] = len(str)
			}
		}
	}

	fmt.Printf("\nDataFrame: %d rows x %d columns\n", total, len(s.Columns))
	var b strings.Builder
	for i, c := range s.Columns {
		fmt.Fprintf(&b, "%-*s  ", widths[i], c)
	}
	b.WriteByte('\n')
	for i := range s.Columns {
		b.WriteString(strings.Repeat("-", widths[i]))
		b.WriteString("  ")
	}
	b.WriteByte('\n')
	for _, rowCells := range cells {
		for i, str := range rowCells {
			fmt.Fprintf(&b, "%-*s  ", widths[i], str)
		}
		b.WriteByte('\n')
	}
	fmt.Print(b.String())
	if total > limit {
		fmt.Printf("... %d more rows\n", total-limit)
	}
	fmt.Println()
	return nil
}

// formatCell renders a single Go value for display.
func formatCell(v interface{}, t DataType) string {
	if v == nil {
		return "null"
	}
	switch t {
	case TypeTime:
		if ts, ok := v.(time.Time); ok {
			if ts.IsZero() {
				return "null"
			}
			return ts.Format("2006-01-02 15:04:05")
		}
	case TypeFloat:
		if f, ok := v.(float64); ok {
			if math.IsNaN(f) {
				return "NaN"
			}
			return fmt.Sprintf("%.4f", f)
		}
	case TypeInt:
		if n, ok := v.(int64); ok {
			return fmt.Sprintf("%d", n)
		}
	}
	return fmt.Sprintf("%v", v)
}
