# GoData

A lightweight, lazy DataFrame library for Go, built on DuckDB.  
Inspired by pandas, with a focus on simplicity and performance.

> ⚠️ **Status: Unstable / Work in Progress** — GoData is under active development. The API may change without notice, and you should expect bugs. Not yet recommended for production use.

## Features

- **Lazy evaluation** — operations build up a SQL query that only runs when you read data
- **Multiple data sources** — load from maps, structs, or CSV (file or string)
- **Rich transformations** — Select, Filter, Where, Sort, Rename, Drop, WithColumn
- **Aggregations** — GroupBy and whole-frame Agg, powered by DuckDB
- **Joins & concatenation** — Join and Concat
- **Inspect & summarize** — view the generated SQL, Describe (via DuckDB's SUMMARIZE), Materialize
- **Familiar API** — pandas-like ergonomics

## Installation

```bash
go get github.com/DomArruda/GoData
```

GoData depends on DuckDB via cgo, so **cgo must be enabled** to build. It's on by default, but if you've disabled it (or are cross-compiling), set it explicitly:

```bash
export CGO_ENABLED=1
go build ./...
```

You'll also need a C compiler (gcc or clang) installed.

## Quick Start

```go
import "github.com/DomArruda/GoData/dataframe"
```

### Creating DataFrames

Load data from maps, structs, or CSV:

```go
// From a slice of maps
people := []map[string]any{
    {"name": "Alice", "age": 30, "city": "New York", "salary": 85000.5},
    {"name": "Bob", "age": 25, "city": "London", "salary": 72000.0},
}
df, err := dataframe.FromMaps(people)

// From structs (use `df` tags to map fields to columns)
type Person struct {
    Name   string    `df:"name"`
    Age    int       `df:"age"`
    City   string    `df:"city"`
    Salary float64   `df:"salary"`
    Joined time.Time `df:"joined"`
}
df, err := dataframe.FromStructs(structData)

// From a CSV file or a CSV string
df, err := dataframe.ReadCSV("data.csv")
df, err := dataframe.ReadCSVString(csvData)
```

### Transformations

Operations are lazy and chainable — they build up a query that runs only when you read:

```go
df, err := df.Select("name", "age", "city", "salary")
df, err = df.Rename("salary", "annual_salary")
df, err = df.Drop("city")
df, err = df.WithColumn("salary_k", "annual_salary / 1000")
```

`WithColumn` takes a SQL expression, so it can reference other columns.

### Filtering & Sorting

```go
df, err := df.Filter("age > 28")
df = df.Where("city = 'New York' OR city = 'London'") // Where returns the frame (no error)

// The second slice sets direction per column: true = ascending, false = descending
df, err := df.Sort([]string{"salary"}, []bool{false})
```

### Aggregations

```go
// Group and aggregate. Use `As` to name the output column.
grouped, err := df.GroupBy(
    []string{"city"},
    dataframe.Aggregation{Column: "salary", Func: "avg", As: "avg_salary"},
    dataframe.Aggregation{Column: "salary", Func: "max", As: "max_salary"},
    dataframe.Aggregation{Column: "name", Func: "count", As: "count"},
)

// Aggregate the whole frame. Without `As`, output columns are named <column>_<func>.
agg, err := df.Agg(
    dataframe.Aggregation{Column: "salary", Func: "mean"},   // -> salary_mean
    dataframe.Aggregation{Column: "age", Func: "median"},    // -> age_median
)
```

### Joins & Concatenation

```go
// The third arg is the join type (e.g. "left"); the last two are the
// left/right key columns. Overlapping non-key columns from the right
// frame get a _right suffix.
joined, err := df.Join(other, "left", "name", "name")

// Concatenate rows (union by column name).
combined, err := df.Concat(other)
```

### Reading Data

Read results in whatever form you need:

```go
// Print to stdout (optionally limit rows)
df.Print()
df.Head(5).Print()
df.Print(5)

// Collect all rows into a slice
rows, err := df.Collect()

// Iterate row by row; return false from the callback to stop early
df.Iterate(func(row dataframe.Row) (bool, error) {
    fmt.Printf("%v | age=%v\n", row["name"], row["age"])
    return true, nil
})

// Pull a single scalar value (the first cell of the frame)
meanSalary, err := agg.Float()
```

### Inspecting & Summarizing

```go
// See the generated SQL for any lazy frame
fmt.Println(grouped.SQL())

// Summary statistics via DuckDB's SUMMARIZE: one row per column with
// count, min, max, avg, std, approximate quantiles (q25/q50/q75),
// null percentage, and an approximate distinct count. Covers all
// columns, not just numeric ones.
desc, err := df.Describe()
desc.Print()

// Materialize a lazy frame to cache the result
mat, err := grouped.Materialize()
defer mat.Release()
```

## Memory Management

DataFrames hold underlying resources — call `Release()` when you're done with source frames and materialized results:

```go
df, err := dataframe.ReadCSVString(csvData)
defer df.Release()

mat, err := grouped.Materialize()
defer mat.Release()
```

## License

[Add your license here]
