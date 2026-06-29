# GoData

A lightweight, lazy DataFrame library for Go, built on DuckDB.  
Inspired by pandas, with a focus on simplicity and performance.

> ⚠️ **Status: Unstable / Work in Progress** — GoData is under active development. The API may change without notice, and you should expect bugs. Not yet recommended for production use.

## Features

- **Lazy evaluation** — Operations build SQL queries that only run when you read data
- **Multiple data sources** — Load from CSV, maps, and structs
- **Rich transformations** — Select, Filter, Where, GroupBy, Join, Concat, Sort, Rename, Drop, WithColumn
- **Fast aggregations** using DuckDB's engine
- **Easy to use** — Familiar pandas-like API

## Installation

```bash
go get github.com/DomArruda/GoData
```

GoData depends on DuckDB via cgo, so **cgo must be enabled** to build. It's on by default, but if you've disabled it (or are cross-compiling), set it explicitly:

```bash
export CGO_ENABLED=1
go build ./...
```

You'll also need a C compiler (gcc or clang) available on your system.

## Quick Start

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

// From a CSV file or string
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

### Filtering & Sorting

```go
df, err := df.Filter("age > 28")
df = df.Where("city = 'New York' OR city = 'London'")

// Sort by salary; the bool slice sets the sort direction per column
df, err := df.Sort([]string{"salary"}, []bool{false})
```

### Aggregations

Group and aggregate, or aggregate the whole frame:

```go
grouped, err := df.GroupBy(
    []string{"city"},
    dataframe.Aggregation{Column: "salary", Func: "avg", As: "avg_salary"},
    dataframe.Aggregation{Column: "salary", Func: "max", As: "max_salary"},
    dataframe.Aggregation{Column: "name", Func: "count", As: "count"},
)

agg, err := df.Agg(
    dataframe.Aggregation{Column: "salary", Func: "mean"},
    dataframe.Aggregation{Column: "age", Func: "median"},
)
```

### Joins & Concatenation

```go
joined, err := df.Join(other, "left", "name", "name")
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

// Iterate row by row
df.Iterate(func(row dataframe.Row) (bool, error) {
    fmt.Printf("%s | age=%v\n", row["name"], row["age"])
    return true, nil // return false to stop early
})

// Pull a single scalar value
meanSalary, err := agg.Float()
```

### Inspecting & Materializing

```go
// See the generated SQL for any lazy frame
fmt.Println(grouped.SQL())

// Summary statistics
desc, err := df.Describe()
desc.Print()

// Materialize a lazy frame to cache the result
mat, err := grouped.Materialize()
defer mat.Release()
```



## License

[Add your license here]
