package dataframe

import (
	"fmt"
	"strings"
)

// This file handles reading and writing files. The big win from the DuckDB
// backend is here: instead of hand-rolling CSV parsing, type inference, and a
// reflection-based Parquet reader, we delegate to DuckDB's native, vectorized
// readers (read_csv_auto, read_json_auto, read_parquet) and its COPY writer.
//
// We import files into a base table (CREATE TABLE ... AS SELECT ... FROM
// read_*()) so the data is owned by DuckDB and downstream ops stay lazy.

// ReadCSV loads a CSV file into a DataFrame using DuckDB's automatic CSV
// reader, which infers column types and handles quoting, delimiters, and
// headers. This replaces the original manual parser and its bespoke NA
// handling (DuckDB recognizes standard null tokens).

func ReadCSV(path string) (*DataFrame, error) {
	e, err := getDefaultEngine()
	if err != nil {
		return nil, err
	}
	return ReadCSVEngine(e, path)
}

// ReadCSVEngine is ReadCSV against a specific engine.
func ReadCSVEngine(e *Engine, path string) (*DataFrame, error) {
	src := fmt.Sprintf("read_csv_auto(%s)", quoteLit(path))
	return importInto(e, src)
}

// ReadCSVString loads CSV data from an in-memory string. DuckDB reads files,
// not Go strings, so we parse the text ourselves into rows and materialize
// them. For large inputs prefer ReadCSV against a file so DuckDB's vectorized
// reader does the work.
func ReadCSVString(csvData string) (*DataFrame, error) {
	e, err := getDefaultEngine()
	if err != nil {
		return nil, err
	}
	return ReadCSVStringEngine(e, csvData)
}

// ReadCSVStringEngine is ReadCSVString against a specific engine.
func ReadCSVStringEngine(e *Engine, csvData string) (*DataFrame, error) {
	// Round-trip through a temp file so DuckDB's type inference applies. This
	// keeps behavior identical to ReadCSV for the same bytes.
	f, cleanup, err := writeTempFile("df-*.csv", []byte(csvData))
	if err != nil {
		return nil, err
	}
	defer cleanup()
	return ReadCSVEngine(e, f)
}

// ReadJSON loads a JSON file (array of objects, or newline-delimited) using
// DuckDB's read_json_auto.
func ReadJSON(path string) (*DataFrame, error) {
	e, err := getDefaultEngine()
	if err != nil {
		return nil, err
	}
	return ReadJSONEngine(e, path)
}

// ReadJSONEngine is ReadJSON against a specific engine.
func ReadJSONEngine(e *Engine, path string) (*DataFrame, error) {
	src := fmt.Sprintf("read_json_auto(%s)", quoteLit(path))
	return importInto(e, src)
}

// ReadJSONBytes loads JSON from an in-memory byte slice by staging it to a temp
// file and reading it with DuckDB.
func ReadJSONBytes(data []byte) (*DataFrame, error) {
	e, err := getDefaultEngine()
	if err != nil {
		return nil, err
	}
	f, cleanup, err := writeTempFile("df-*.json", data)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	return ReadJSONEngine(e, f)
}

// ReadParquet loads a Parquet file using DuckDB's native read_parquet. This
// replaces the original's ~200-line reflection-based reader entirely.
func ReadParquet(path string) (*DataFrame, error) {
	e, err := getDefaultEngine()
	if err != nil {
		return nil, err
	}
	return ReadParquetEngine(e, path)
}

// ReadParquetEngine is ReadParquet against a specific engine.
func ReadParquetEngine(e *Engine, path string) (*DataFrame, error) {
	src := fmt.Sprintf("read_parquet(%s)", quoteLit(path))
	return importInto(e, src)
}

// ReadSQL runs a query against an external database via a DuckDB scanner.
//
// DuckDB can attach external systems, but configuring that generically is
// beyond this helper. Instead, ReadSQL executes the query against the
// DataFrame engine's own DuckDB instance, which is the right call when the data
// already lives in DuckDB (tables you've created, attached databases, etc.).
// For pulling from Postgres/MySQL/SQLite, ATTACH the source first using DuckDB's
// scanner extensions, then call ReadSQL.
func ReadSQL(query string) (*DataFrame, error) {
	e, err := getDefaultEngine()
	if err != nil {
		return nil, err
	}
	return ReadSQLEngine(e, query)
}

// ReadSQLEngine is ReadSQL against a specific engine.
func ReadSQLEngine(e *Engine, query string) (*DataFrame, error) {
	// Wrap the user query as a subquery so the result becomes a materialized
	// table we own. The query is trusted caller-supplied SQL by contract.
	return importInto(e, "("+query+")")
}

// importInto materializes a source relation expression into an owned base table
// and returns a DataFrame over it.
func importInto(e *Engine, sourceRelation string) (*DataFrame, error) {
	name := e.newName("df")
	q := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s",
		quoteIdent(name), sourceRelation)
	if _, err := e.db.Exec(q); err != nil {
		return nil, fmt.Errorf("importing data: %w", err)
	}
	return newOwnedDataFrame(e, name), nil
}

// WriteCSV writes the frame to a CSV file using DuckDB's COPY, including a
// header row. COPY streams directly from the query engine to disk without
// pulling rows into Go.
func (df *DataFrame) WriteCSV(path string) error {
	return df.copyTo(path, "(FORMAT CSV, HEADER)")
}

// WriteJSON writes the frame to a newline-delimited / array JSON file via COPY.
func (df *DataFrame) WriteJSON(path string) error {
	return df.copyTo(path, "(FORMAT JSON, ARRAY true)")
}

// WriteParquet writes the frame to a Parquet file via COPY with Snappy
// compression (DuckDB's default), replacing the original's manual writer.
func (df *DataFrame) WriteParquet(path string) error {
	return df.copyTo(path, "(FORMAT PARQUET, COMPRESSION SNAPPY)")
}

// copyTo runs COPY ( <relation> ) TO '<path>' WITH <options>.
func (df *DataFrame) copyTo(path, options string) error {
	q := fmt.Sprintf("COPY (SELECT * FROM (%s) AS _df) TO %s %s",
		df.relation, quoteLit(path), options)
	if _, err := df.eng.db.Exec(q); err != nil {
		return fmt.Errorf("writing %s: %w", strings.TrimSpace(options), err)
	}
	return nil
}
