package main

import (
	"fmt"
	"log"
	"time"

	"github.com/DomArruda/GoData/dataframe"
)

func main() {
	fmt.Println("=== DataFrame Demo - All Key Features ===\n")

	// ========== 1. CREATE DATA ==========
	people := []map[string]any{
		{"name": "Alice", "age": 30, "city": "New York", "salary": 85000.5, "joined": "2023-01-15"},
		{"name": "Bob", "age": 25, "city": "London", "salary": 72000.0, "joined": "2022-11-20"},
		{"name": "Charlie", "age": 35, "city": "New York", "salary": 95000.0, "joined": "2021-06-10"},
		{"name": "Diana", "age": 28, "city": "Paris", "salary": 68000.0, "joined": "2023-03-01"},
	}
	dfMaps, err := dataframe.FromMaps(people)
	if err != nil {
		log.Fatal(err)
	}

	type Person struct {
		Name   string    `df:"name"`
		Age    int       `df:"age"`
		City   string    `df:"city"`
		Salary float64   `df:"salary"`
		Joined time.Time `df:"joined"`
	}
	structData := []Person{
		{"Eve", 32, "Berlin", 78000, time.Date(2022, 8, 5, 0, 0, 0, 0, time.UTC)},
		{"Frank", 29, "New York", 88000, time.Date(2023, 2, 14, 0, 0, 0, 0, time.UTC)},
	}
	dfStructs, err := dataframe.FromStructs(structData)
	if err != nil {
		log.Fatal(err)
	}

	csvData := `name,age,city,salary,joined
Grace,31,London,91000,2022-09-12
Henry,27,Paris,65000,2023-05-20
Ivy,34,New York,102000,2021-12-01`
	dfCSV, err := dataframe.ReadCSVString(csvData)
	if err != nil {
		log.Fatal(err)
	}
	defer dfCSV.Release()

	fmt.Println("Created 3 DataFrames successfully")

	// ========== 2. TRANSFORMATIONS (with error checking) ==========
	df1, err := dfMaps.Select("name", "age", "city", "salary")
	if err != nil {
		log.Fatal("Select failed: ", err)
	}

	df1, err = df1.Rename("salary", "annual_salary")
	if err != nil {
		log.Fatal("Rename failed: ", err)
	}

	df1, err = df1.Drop("city")
	if err != nil {
		log.Fatal("Drop failed: ", err)
	}

	df1, err = df1.WithColumn("salary_k", "annual_salary / 1000")
	if err != nil {
		log.Fatal("WithColumn failed: ", err)
	}

	// Filtering examples
	df2, err := dfMaps.Filter("age > 28")
	if err != nil {
		log.Fatal(err)
	}
	df2 = df2.Where("city = 'New York' OR city = 'London'")

	dfSorted, err := dfMaps.Sort([]string{"salary"}, []bool{false})
	if err != nil {
		log.Fatal(err)
	}

	// ========== 3. AGGREGATIONS ==========
	dfGrouped, err := dfMaps.GroupBy(
		[]string{"city"},
		dataframe.Aggregation{Column: "salary", Func: "avg", As: "avg_salary"},
		dataframe.Aggregation{Column: "salary", Func: "max", As: "max_salary"},
		dataframe.Aggregation{Column: "name", Func: "count", As: "count"},
	)
	if err != nil {
		log.Fatal(err)
	}

	dfAgg, err := dfMaps.Agg(
		dataframe.Aggregation{Column: "salary", Func: "mean"},
		dataframe.Aggregation{Column: "age", Func: "median"},
	)
	if err != nil {
		log.Fatal(err)
	}

	// ========== 4. JOIN + CONCAT ==========
	joined, err := dfMaps.Join(dfStructs, "left", "name", "name")
	if err != nil {
		log.Fatal(err)
	}

	combined, err := dfMaps.Concat(dfStructs)
	if err != nil {
		log.Fatal(err)
	}
	_ = combined // just to show it works

	// ========== 5. READING DATA ==========
	fmt.Println("\n=== Original data (Head) ===")
	if err := dfMaps.Head(5).Print(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("\n=== Grouped by city ===")
	if err := dfGrouped.Print(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("\n=== Whole-frame Agg ===")
	if err := dfAgg.Print(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("\n=== Left Join result ===")
	if err := joined.Print(); err != nil {
		log.Fatal(err)
	}

	// Collect
	rows, err := dfSorted.Collect()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nCollected %d rows\n", len(rows))

	// Iterate
	fmt.Println("\n=== Iterate example ===")
	_ = dfMaps.Iterate(func(row dataframe.Row) (bool, error) {
		fmt.Printf("  %s | age=%v | city=%v\n", row["name"], row["age"], row["city"])
		return true, nil
	})

	// Scalar
	meanSal, err := dfAgg.Float()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nMean salary (scalar): %.2f\n", meanSal)

	// SQL inspection
	fmt.Println("\n=== Generated SQL (grouped) ===")
	fmt.Println(dfGrouped.SQL())

	// ========== 6. ADVANCED ==========
	desc, err := dfMaps.Describe()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\n=== DESCRIBE ===")
	desc.Print()

	// Materialize example
	mat, err := dfGrouped.Materialize()
	if err != nil {
		log.Fatal(err)
	}
	defer mat.Release()
	fmt.Println("\nMaterialized grouped frame successfully")

	fmt.Println("\n=== Demo finished successfully ===")


	// Reading a csv....

	fmt.Println("Reading from a csv....")

	const csvFile = "data.csv"
	csvDf, err := dataframe.ReadCSV(csvFile)
	if err != nil {
		log.Fatal(err)
	}

	if err := csvDf.Print(5); err != nil {
		log.Fatal(err)
	}

	fmt.Println("\n=== Demo finished successfully ===")
}
