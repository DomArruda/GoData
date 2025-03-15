package dataframe

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/mattn/go-sqlite3"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"

	"gonum.org/v1/gonum/mat"
	"gonum.org/v1/gonum/stat"
)

// DataType represents the type of data in a column

// core.go
type DataType int

const (
	TypeInt DataType = iota
	TypeFloat
	TypeString
	TypeTime
)

// DataFrame represents a 2D tabular data structure with Gonum integration
type DataFrame struct {
	Columns     []string       // Column names
	ColTypes    []DataType     // Type of each column
	NumericData *mat.Dense     // Numeric data stored in a Gonum matrix
	StringData  [][]string     // String data stored separately
	TimeData    [][]time.Time  // Time data stored separately
	Rows        int            // Number of rows
	Cols        int            // Total number of columns
	NumericCols int            // Number of numeric columns
	StringCols  int            // Number of string columns
	TimeCols    int            // Number of time columns
	ColIndex    map[string]int // Maps column names to indices
	mu          sync.RWMutex   // For thread safety
}

// Person is an example struct for type switching
type Person struct {
	Name  string
	Age   int
	Score float64
	Birth time.Time
}

// NewDataFrame creates a new empty DataFrame
func NewDataFrame() *DataFrame {
	return &DataFrame{
		ColIndex: make(map[string]int),
	}
}

// NewDataFrameFromInterface creates a DataFrame from an interface{} using type switching
func NewDataFrameFromInterface(data interface{}) (*DataFrame, error) {

	df := NewDataFrame()

	switch v := data.(type) {
	case []map[string]interface{}:
		if len(v) == 0 {
			return df, nil
		}
		columns := make([]string, 0)
		for k := range v[0] {
			columns = append(columns, k)
		}
		sort.Strings(columns)
		df.Cols = len(columns)
		df.Columns = columns
		df.Rows = len(v)
		df.ColTypes = make([]DataType, df.Cols)

		typeCounts := make([]map[DataType]int, df.Cols)
		for i := range typeCounts {
			typeCounts[i] = map[DataType]int{TypeInt: 0, TypeFloat: 0, TypeString: 0, TypeTime: 0}
		}
		for _, row := range v {
			for j, col := range columns {
				if val, ok := row[col]; ok && val != nil {
					switch val := val.(type) {
					case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
						typeCounts[j][TypeInt]++
					case float32, float64:
						typeCounts[j][TypeFloat]++
					case time.Time:
						typeCounts[j][TypeTime]++
					case string:
						if _, err := time.Parse(time.RFC3339, val); err == nil {
							typeCounts[j][TypeTime]++
						} else {
							typeCounts[j][TypeString]++
						}
					default:
						typeCounts[j][TypeString]++
					}
				}
			}
		}
		for i, counts := range typeCounts {
			if counts[TypeString] > 0 {
				df.ColTypes[i] = TypeString
				df.StringCols++
			} else if counts[TypeTime] > 0 {
				df.ColTypes[i] = TypeTime
				df.TimeCols++
			} else if counts[TypeFloat] > 0 {
				df.ColTypes[i] = TypeFloat
				df.NumericCols++
			} else if counts[TypeInt] > 0 {
				df.ColTypes[i] = TypeInt
				df.NumericCols++
			} else {
				df.ColTypes[i] = TypeString
				df.StringCols++
			}
			df.ColIndex[columns[i]] = i
		}

		if df.StringCols > 0 {
			df.StringData = make([][]string, df.StringCols)
			for i := range df.StringData {
				df.StringData[i] = make([]string, df.Rows)
			}
		}
		if df.TimeCols > 0 {
			df.TimeData = make([][]time.Time, df.TimeCols)
			for i := range df.TimeData {
				df.TimeData[i] = make([]time.Time, df.Rows)
			}
		}
		if df.NumericCols > 0 {
			numericData := make([]float64, df.Rows*df.NumericCols)
			sIdx := 0
			tIdx := 0
			nIdx := 0
			for colIdx, col := range columns {
				for rowIdx, row := range v {
					val, ok := row[col]
					if !ok || val == nil {
						if df.ColTypes[colIdx] == TypeString {
							df.StringData[sIdx][rowIdx] = "null"
						} else if df.ColTypes[colIdx] == TypeTime {
							df.TimeData[tIdx][rowIdx] = time.Time{}
						} else {
							numericData[rowIdx*df.NumericCols+nIdx] = math.NaN()
						}
					} else if df.ColTypes[colIdx] == TypeString {
						df.StringData[sIdx][rowIdx] = fmt.Sprintf("%v", val)
					} else if df.ColTypes[colIdx] == TypeTime {
						switch v := val.(type) {
						case time.Time:
							df.TimeData[tIdx][rowIdx] = v
						case string:
							if t, err := time.Parse(time.RFC3339, v); err == nil {
								df.TimeData[tIdx][rowIdx] = t
							}
						}
					} else {
						switch x := val.(type) {
						case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
							numericData[rowIdx*df.NumericCols+nIdx] = float64(reflect.ValueOf(x).Int())
						case float32, float64:
							numericData[rowIdx*df.NumericCols+nIdx] = float64(reflect.ValueOf(x).Float())
						default:
							numericData[rowIdx*df.NumericCols+nIdx] = math.NaN()
						}
					}
				}
				if df.ColTypes[colIdx] == TypeString {
					sIdx++
				} else if df.ColTypes[colIdx] == TypeTime {
					tIdx++
				} else {
					nIdx++
				}
			}
			df.NumericData = mat.NewDense(df.Rows, df.NumericCols, numericData)
		}

	case []Person:
		if len(v) == 0 {
			return df, nil
		}
		columns := []string{"Name", "Age", "Score", "Birth"}
		types := []DataType{TypeString, TypeInt, TypeFloat, TypeTime}
		df.Rows = len(v)
		df.Cols = len(columns)
		df.Columns = columns
		df.ColTypes = types
		df.StringCols = 1
		df.NumericCols = 2
		df.TimeCols = 1
		for i, col := range columns {
			df.ColIndex[col] = i
		}

		df.StringData = make([][]string, df.StringCols)
		df.StringData[0] = make([]string, df.Rows)
		numericData := make([]float64, df.Rows*df.NumericCols)
		df.TimeData = make([][]time.Time, df.TimeCols)
		df.TimeData[0] = make([]time.Time, df.Rows)
		for i, p := range v {
			df.StringData[0][i] = p.Name
			numericData[i*df.NumericCols+0] = float64(p.Age)
			numericData[i*df.NumericCols+1] = p.Score
			df.TimeData[0][i] = p.Birth
		}
		df.NumericData = mat.NewDense(df.Rows, df.NumericCols, numericData)

	default:
		return nil, fmt.Errorf("unsupported type: %T", data)
	}

	return df, nil
}

// NewDataFrameFromData creates a DataFrame with inferred properties
func NewDataFrameFromData(columns []string, types []DataType, stringData [][]string, timeData [][]time.Time, numericData []float64) (*DataFrame, error) {
	df := NewDataFrame()

	if len(columns) != len(types) {
		return nil, fmt.Errorf("number of columns (%d) must match number of types (%d)", len(columns), len(types))
	}
	df.Cols = len(columns)
	df.Columns = columns
	df.ColTypes = types

	for _, t := range types {
		switch t {
		case TypeString:
			df.StringCols++
		case TypeTime:
			df.TimeCols++
		default:
			df.NumericCols++
		}
	}

	df.ColIndex = make(map[string]int)
	for i, col := range columns {
		df.ColIndex[col] = i
	}

	if df.StringCols > 0 {
		if len(stringData) != df.StringCols {
			return nil, fmt.Errorf("number of string columns in data (%d) must match StringCols (%d)", len(stringData), df.StringCols)
		}
		rows := len(stringData[0])
		for i, col := range stringData {
			if len(col) != rows {
				return nil, fmt.Errorf("inconsistent row count in string data: column %d has %d rows, expected %d", i, len(col), rows)
			}
		}
		df.Rows = rows
		df.StringData = stringData
	}

	if df.TimeCols > 0 {
		if len(timeData) != df.TimeCols {
			return nil, fmt.Errorf("number of time columns in data (%d) must match TimeCols (%d)", len(timeData), df.TimeCols)
		}
		rows := len(timeData[0])
		for i, col := range timeData {
			if len(col) != rows {
				return nil, fmt.Errorf("inconsistent row count in time data: column %d has %d rows, expected %d", i, len(col), rows)
			}
		}
		if df.Rows == 0 {
			df.Rows = rows
		} else if df.Rows != rows {
			return nil, fmt.Errorf("row count mismatch between string (%d) and time (%d) data", df.Rows, rows)
		}
		df.TimeData = timeData
	}

	if df.NumericCols > 0 {
		expectedSize := df.Rows * df.NumericCols
		if len(numericData) != expectedSize && df.Rows > 0 {
			return nil, fmt.Errorf("numeric data size (%d) does not match expected size (%d rows * %d cols = %d)", len(numericData), df.Rows, df.NumericCols, expectedSize)
		}
		if df.Rows == 0 {
			if len(numericData)%df.NumericCols != 0 {
				return nil, fmt.Errorf("numeric data length (%d) must be divisible by number of numeric columns (%d)", len(numericData), df.NumericCols)
			}
			df.Rows = len(numericData) / df.NumericCols
		}
		df.NumericData = mat.NewDense(df.Rows, df.NumericCols, numericData)
	}

	return df, nil
}

// ReadCSVStringToDataFrame reads a CSV string into a DataFrame
func ReadCSVStringToDataFrame(csvString string) (*DataFrame, error) {
	return readCSVToDataFrame(strings.NewReader(csvString))
}

// ReadCSVFileToDataFrame reads a CSV file into a DataFrame
func ReadCSVFileToDataFrame(filename string) (*DataFrame, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()
	return readCSVToDataFrame(file)
}

// readCSVToDataFrame processes CSV data from an io.Reader
func readCSVToDataFrame(r io.Reader) (*DataFrame, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true
	headers, err := reader.Read()
	if err != nil {

		return nil, fmt.Errorf("error reading headers: %v", err)

	}

	data := make([][]string, 0)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}

	if len(data) == 0 {
		return NewDataFrame(), nil
	}

	df := NewDataFrame()
	df.Rows = len(data)
	df.Cols = len(headers)
	df.Columns = headers
	df.ColTypes = make([]DataType, df.Cols)
	df.ColIndex = make(map[string]int)

	typeCounts := make([]map[DataType]int, df.Cols)
	for i := range typeCounts {
		typeCounts[i] = map[DataType]int{TypeInt: 0, TypeFloat: 0, TypeString: 0, TypeTime: 0}
	}
	for _, row := range data {
		for j, val := range row {
			if val == "" || val == "NA" || val == "NaN" || val == "null" || val == "NULL" {
				continue
			}
			if _, err := strconv.ParseInt(val, 10, 64); err == nil {
				typeCounts[j][TypeInt]++
			} else if _, err := strconv.ParseFloat(val, 64); err == nil {
				typeCounts[j][TypeFloat]++
			} else if _, err := time.Parse("2006-01-02", val); err == nil {
				typeCounts[j][TypeTime]++
			} else if _, err := time.Parse(time.RFC3339, val); err == nil {
				typeCounts[j][TypeTime]++
			} else {
				typeCounts[j][TypeString]++
			}
		}
	}

	for i, counts := range typeCounts {
		if counts[TypeString] > 0 {
			df.ColTypes[i] = TypeString
			df.StringCols++
		} else if counts[TypeTime] > 0 {
			df.ColTypes[i] = TypeTime
			df.TimeCols++
		} else if counts[TypeFloat] > 0 {
			df.ColTypes[i] = TypeFloat
			df.NumericCols++
		} else if counts[TypeInt] > 0 {
			df.ColTypes[i] = TypeInt
			df.NumericCols++
		} else {
			df.ColTypes[i] = TypeString
			df.StringCols++
		}
		df.ColIndex[headers[i]] = i
	}

	numericData := make([]float64, df.Rows*df.NumericCols)
	stringData := make([][]string, df.StringCols)
	timeData := make([][]time.Time, df.TimeCols)
	for i := range stringData {
		stringData[i] = make([]string, df.Rows)
	}
	for i := range timeData {
		timeData[i] = make([]time.Time, df.Rows)
	}

	nIdx := 0
	sIdx := 0
	tIdx := 0
	for colIdx, colType := range df.ColTypes {
		for rowIdx, row := range data {
			val := row[colIdx]
			switch colType {
			case TypeInt, TypeFloat:
				if val == "" || val == "NA" || val == "NaN" || val == "null" || val == "NULL" {
					numericData[rowIdx*df.NumericCols+nIdx] = math.NaN()
				} else {
					f, _ := strconv.ParseFloat(val, 64)
					numericData[rowIdx*df.NumericCols+nIdx] = f
				}
			case TypeString:
				stringData[sIdx][rowIdx] = val
			case TypeTime:
				if val == "" || val == "NA" || val == "NaN" || val == "null" || val == "NULL" {
					timeData[tIdx][rowIdx] = time.Time{}
				} else if t, err := time.Parse("2006-01-02", val); err == nil {
					timeData[tIdx][rowIdx] = t
				} else if t, err := time.Parse(time.RFC3339, val); err == nil {
					timeData[tIdx][rowIdx] = t
				}
			}
		}
		if colType == TypeString {
			sIdx++
		} else if colType == TypeTime {
			tIdx++
		} else {
			nIdx++
		}
	}

	if df.NumericCols > 0 {
		df.NumericData = mat.NewDense(df.Rows, df.NumericCols, numericData)
	}
	if df.StringCols > 0 {
		df.StringData = stringData
	}
	if df.TimeCols > 0 {
		df.TimeData = timeData
	}

	return df, nil
}

// Print displays the DataFrame
func (df *DataFrame) Print() {
	df.mu.RLock()
	defer df.mu.RUnlock()
	if df.Rows == 0 || df.Cols == 0 {
		fmt.Println("Empty DataFrame")
		return
	}
	fmt.Printf("\nDataFrame: %d rows x %d columns\n", df.Rows, df.Cols)
	maxRows := min(10, df.Rows)
	colWidths := make([]int, df.Cols)
	for i, col := range df.Columns {
		colWidths[i] = len(col)
	}
	for i := 0; i < maxRows; i++ {
		for j := 0; j < df.Cols; j++ {
			val := df.getValue(i, j)
			if len(val) > colWidths[j] {
				colWidths[j] = len(val)
			}
		}
	}
	for i, col := range df.Columns {
		fmt.Printf("%-*s ", colWidths[i]+2, col)
	}
	fmt.Println()
	for i := 0; i < df.Cols; i++ {
		fmt.Print(strings.Repeat("-", colWidths[i]+2) + " ")
	}
	fmt.Println()
	for i := 0; i < maxRows; i++ {
		for j := 0; j < df.Cols; j++ {
			fmt.Printf("%-*s ", colWidths[j]+2, df.getValue(i, j))
		}
		fmt.Println()
	}
	if df.Rows > maxRows {
		fmt.Printf("... %d more rows\n", df.Rows-maxRows)
	}
	fmt.Println()
}

// getValue retrieves a value at row i, column j as a string
func (df *DataFrame) getValue(i, j int) string {
	switch df.ColTypes[j] {
	case TypeString:
		sIdx := 0
		for k := 0; k < j; k++ {
			if df.ColTypes[k] == TypeString {
				sIdx++
			}
		}
		return df.StringData[sIdx][i]
	case TypeTime:
		tIdx := 0
		for k := 0; k < j; k++ {
			if df.ColTypes[k] == TypeTime {
				tIdx++
			}
		}
		if df.TimeData[tIdx][i].IsZero() {
			return "null"
		}
		return df.TimeData[tIdx][i].Format("2006-01-02")
	default:
		nIdx := 0
		for k := 0; k < j; k++ {
			if df.ColTypes[k] != TypeString && df.ColTypes[k] != TypeTime {
				nIdx++
			}
		}
		val := df.NumericData.At(i, nIdx)
		if math.IsNaN(val) {
			return "NaN"
		}
		if df.ColTypes[j] == TypeInt && val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%.4f", val)
	}
}

// Filter applies a condition to filter rows with parallelism
func (df *DataFrame) Filter(condition interface{}, verbose ...bool) *DataFrame {
	df.mu.RLock()
	defer df.mu.RUnlock()
	newDF := NewDataFrame()
	if df.Rows == 0 {
		newDF.Columns = append([]string{}, df.Columns...)
		newDF.ColTypes = append([]DataType{}, df.ColTypes...)
		newDF.Cols = df.Cols
		newDF.NumericCols = df.NumericCols
		newDF.StringCols = df.StringCols
		newDF.TimeCols = df.TimeCols
		newDF.ColIndex = make(map[string]int)
		for k, v := range df.ColIndex {
			newDF.ColIndex[k] = v
		}
		return newDF
	}

	predicate := df.conditionToFunction(condition)
	bitmap := make([]bool, df.Rows)
	var wg sync.WaitGroup
	chunkSize := (df.Rows + runtime.NumCPU() - 1) / runtime.NumCPU()

	for start := 0; start < df.Rows; start += chunkSize {
		end := start + chunkSize
		if end > df.Rows {
			end = df.Rows
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			for i := s; i < e; i++ {
				bitmap[i] = predicate(i)
			}
		}(start, end)
	}
	wg.Wait()

	indices := make([]int, 0, df.Rows)
	for i, keep := range bitmap {
		if keep {
			indices = append(indices, i)
		}
	}

	newDF.Rows = len(indices)
	newDF.Columns = append([]string{}, df.Columns...)
	newDF.ColTypes = append([]DataType{}, df.ColTypes...)
	newDF.Cols = df.Cols
	newDF.NumericCols = df.NumericCols
	newDF.StringCols = df.StringCols
	newDF.TimeCols = df.TimeCols
	newDF.ColIndex = make(map[string]int)
	for k, v := range df.ColIndex {
		newDF.ColIndex[k] = v
	}

	if df.NumericCols > 0 {
		newNumeric := make([]float64, len(indices)*df.NumericCols)
		for i, idx := range indices {
			for j := 0; j < df.NumericCols; j++ {
				newNumeric[i*df.NumericCols+j] = df.NumericData.At(idx, j)
			}
		}
		newDF.NumericData = mat.NewDense(len(indices), df.NumericCols, newNumeric)
	}
	if df.StringCols > 0 {
		newDF.StringData = make([][]string, df.StringCols)
		for i := range newDF.StringData {
			newDF.StringData[i] = make([]string, len(indices))
			sIdx := 0
			for _, t := range df.ColTypes {
				if t == TypeString {
					if sIdx == i {
						for k, idx := range indices {
							newDF.StringData[i][k] = df.StringData[sIdx][idx]
						}
						break
					}
					sIdx++
				}
			}
		}
	}
	if df.TimeCols > 0 {
		newDF.TimeData = make([][]time.Time, df.TimeCols)
		for i := range newDF.TimeData {
			newDF.TimeData[i] = make([]time.Time, len(indices))
			tIdx := 0
			for _, t := range df.ColTypes {
				if t == TypeTime {
					if tIdx == i {
						for k, idx := range indices {
							newDF.TimeData[i][k] = df.TimeData[tIdx][idx]
						}
						break
					}
					tIdx++
				}
			}
		}
	}
	return newDF
}

// conditionToFunction converts a condition to a row predicate function
func (df *DataFrame) conditionToFunction(condition interface{}) func(int) bool {
	switch cond := condition.(type) {
	case string:
		parts := strings.Fields(cond)
		if len(parts) != 3 {
			return func(int) bool { return true }
		}
		colName, op, valStr := parts[0], parts[1], parts[2]
		colIdx, ok := df.ColIndex[colName]
		if !ok {
			return func(int) bool { return true }
		}
		switch df.ColTypes[colIdx] {
		case TypeString:
			return func(row int) bool {
				sIdx := 0
				for i := 0; i < colIdx; i++ {
					if df.ColTypes[i] == TypeString {
						sIdx++
					}
				}
				switch op {
				case "=":
					return df.StringData[sIdx][row] == valStr
				case ">":
					return df.StringData[sIdx][row] > valStr
				case "<":
					return df.StringData[sIdx][row] < valStr
				default:
					return true
				}
			}
		case TypeTime:
			t, _ := time.Parse("2006-01-02", valStr)
			tIdx := 0
			for i := 0; i < colIdx; i++ {
				if df.ColTypes[i] == TypeTime {
					tIdx++
				}
			}
			return func(row int) bool {
				switch op {
				case ">":
					return df.TimeData[tIdx][row].After(t)
				case "<":
					return df.TimeData[tIdx][row].Before(t)
				case "=":
					return df.TimeData[tIdx][row].Equal(t)
				default:
					return true
				}
			}
		default:
			nIdx := 0
			for i := 0; i < colIdx; i++ {
				if df.ColTypes[i] != TypeString && df.ColTypes[i] != TypeTime {
					nIdx++
				}
			}
			val, _ := strconv.ParseFloat(valStr, 64)
			return func(row int) bool {
				v := df.NumericData.At(row, nIdx)
				switch op {
				case ">":
					return v > val
				case "<":
					return v < val
				case "=":
					return v == val
				default:
					return true
				}
			}
		}
	case func(int) bool:
		return cond
	default:
		return func(int) bool { return true }
	}
}

// GroupBy groups data and applies aggregations
func (df *DataFrame) GroupBy(groupCols, numericalCols, aggs []string, verbose ...bool) (*DataFrame, error) {

	groups := make(map[string][]int)
	var groupOrder []string
	for i := 0; i < df.Rows; i++ {
		keyParts := make([]string, len(groupCols))
		for j, col := range groupCols {
			idx := df.ColIndex[col]
			switch df.ColTypes[idx] {
			case TypeString:
				sIdx := 0
				for k := 0; k < idx; k++ {
					if df.ColTypes[k] == TypeString {
						sIdx++
					}
				}
				keyParts[j] = df.StringData[sIdx][i]
			case TypeTime:
				tIdx := 0
				for k := 0; k < idx; k++ {
					if df.ColTypes[k] == TypeTime {
						tIdx++
					}
				}
				keyParts[j] = df.TimeData[tIdx][i].Format("2006-01-02")
			default:
				nIdx := 0
				for k := 0; k < idx; k++ {
					if df.ColTypes[k] != TypeString && df.ColTypes[k] != TypeTime {
						nIdx++
					}
				}
				keyParts[j] = fmt.Sprintf("%v", df.NumericData.At(i, nIdx))
			}
		}
		key := strings.Join(keyParts, "|")
		if _, exists := groups[key]; !exists {
			groupOrder = append(groupOrder, key)
		}
		groups[key] = append(groups[key], i)
	}

	newDF := NewDataFrame()
	newDF.Rows = len(groups)
	newDF.Cols = len(groupCols) + len(numericalCols)*len(aggs)
	newDF.Columns = append([]string{}, groupCols...)
	newDF.ColTypes = make([]DataType, newDF.Cols)
	newDF.ColIndex = make(map[string]int)

	for i, col := range groupCols {
		newDF.ColTypes[i] = df.ColTypes[df.ColIndex[col]]
		newDF.ColIndex[col] = i
		switch newDF.ColTypes[i] {
		case TypeString:
			newDF.StringCols++
		case TypeTime:
			newDF.TimeCols++
		default:
			newDF.NumericCols++
		}
	}

	stringData := make([][]string, newDF.StringCols)
	timeData := make([][]time.Time, newDF.TimeCols)
	sIdx := 0
	tIdx := 0
	for i := range groupCols {
		switch newDF.ColTypes[i] {
		case TypeString:
			stringData[sIdx] = make([]string, len(groups))
			for idx, key := range groupOrder {
				parts := strings.Split(key, "|")
				stringData[sIdx][idx] = parts[i]
			}
			sIdx++
		case TypeTime:
			timeData[tIdx] = make([]time.Time, len(groups))
			for idx, key := range groupOrder {
				parts := strings.Split(key, "|")
				if t, err := time.Parse("2006-01-02", parts[i]); err == nil {
					timeData[tIdx][idx] = t
				}
			}
			tIdx++
		}
	}
	newDF.StringData = stringData
	newDF.TimeData = timeData

	numericStart := len(groupCols)
	for i, numCol := range numericalCols {
		colIdx := df.ColIndex[numCol]
		if df.ColTypes[colIdx] == TypeString || df.ColTypes[colIdx] == TypeTime {
			continue
		}
		for j, agg := range aggs {
			newCol := fmt.Sprintf("%s_%s", numCol, agg)
			newDF.Columns = append(newDF.Columns, newCol)
			newDF.ColTypes[numericStart+i*len(aggs)+j] = TypeFloat
			newDF.ColIndex[newCol] = numericStart + i*len(aggs) + j
			newDF.NumericCols++
		}
	}

	if newDF.NumericCols > 0 {
		numericData := make([]float64, len(groups)*newDF.NumericCols)
		for idx, key := range groupOrder {
			indices := groups[key]
			for i, numCol := range numericalCols {
				colIdx := df.ColIndex[numCol]
				if df.ColTypes[colIdx] != TypeString && df.ColTypes[colIdx] != TypeTime {
					nIdx := 0
					for k := 0; k < colIdx; k++ {
						if df.ColTypes[k] != TypeString && df.ColTypes[k] != TypeTime {
							nIdx++
						}
					}
					vals := make([]float64, len(indices))
					for j, row := range indices {
						vals[j] = df.NumericData.At(row, nIdx)
					}
					for j, agg := range aggs {
						offset := idx*newDF.NumericCols + i*len(aggs) + j
						switch agg {
						case "avg":
							numericData[offset] = stat.Mean(vals, nil)
						case "max":
							numericData[offset] = maxFloat(vals)
						case "count":
							numericData[offset] = float64(len(vals))
						}
					}
				}
			}
		}
		newDF.NumericData = mat.NewDense(len(groups), newDF.NumericCols, numericData)
	}
	return newDF, nil
}

// Sort sorts the DataFrame by columns
func (df *DataFrame) Sort(columns []string, ascending []bool, verbose ...bool) (*DataFrame, error) {

	indices := make([]int, df.Rows)
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		for k, colName := range columns {
			colIdx := df.ColIndex[colName]
			switch df.ColTypes[colIdx] {
			case TypeString:
				sIdx := 0
				for m := 0; m < colIdx; m++ {
					if df.ColTypes[m] == TypeString {
						sIdx++
					}
				}
				cmp := strings.Compare(df.StringData[sIdx][indices[i]], df.StringData[sIdx][indices[j]])
				if ascending[k] {
					return cmp < 0
				}
				return cmp > 0
			case TypeTime:
				tIdx := 0
				for m := 0; m < colIdx; m++ {
					if df.ColTypes[m] == TypeTime {
						tIdx++
					}
				}
				t1 := df.TimeData[tIdx][indices[i]]
				t2 := df.TimeData[tIdx][indices[j]]
				if ascending[k] {
					return t1.Before(t2)
				}
				return t1.After(t2)
			default:
				nIdx := 0
				for m := 0; m < colIdx; m++ {
					if df.ColTypes[m] != TypeString && df.ColTypes[m] != TypeTime {
						nIdx++
					}
				}
				v1 := df.NumericData.At(indices[i], nIdx)
				v2 := df.NumericData.At(indices[j], nIdx)
				if ascending[k] {
					return v1 < v2
				}
				return v1 > v2
			}
		}
		return false
	})

	newDF := NewDataFrame()
	newDF.Rows = df.Rows
	newDF.Cols = df.Cols
	newDF.Columns = append([]string{}, df.Columns...)
	newDF.ColTypes = append([]DataType{}, df.ColTypes...)
	newDF.NumericCols = df.NumericCols
	newDF.StringCols = df.StringCols
	newDF.TimeCols = df.TimeCols
	newDF.ColIndex = make(map[string]int)
	for k, v := range df.ColIndex {
		newDF.ColIndex[k] = v
	}

	if df.NumericCols > 0 {
		newNumeric := make([]float64, df.Rows*df.NumericCols)
		for i, idx := range indices {
			for j := 0; j < df.NumericCols; j++ {
				newNumeric[i*df.NumericCols+j] = df.NumericData.At(idx, j)
			}
		}
		newDF.NumericData = mat.NewDense(df.Rows, df.NumericCols, newNumeric)
	}
	if df.StringCols > 0 {
		newDF.StringData = make([][]string, df.StringCols)
		for i := range newDF.StringData {
			newDF.StringData[i] = make([]string, df.Rows)
			for j, idx := range indices {
				newDF.StringData[i][j] = df.StringData[i][idx]
			}
		}
	}
	if df.TimeCols > 0 {
		newDF.TimeData = make([][]time.Time, df.TimeCols)
		for i := range newDF.TimeData {
			newDF.TimeData[i] = make([]time.Time, df.Rows)
			for j, idx := range indices {
				newDF.TimeData[i][j] = df.TimeData[i][idx]
			}
		}
	}
	return newDF, nil
}

// Describe generates descriptive statistics using Gonum
func (df *DataFrame) Describe() *DataFrame {

	stats := []string{"count", "mean", "std", "min", "25%", "50%", "75%", "max"}
	newDF := NewDataFrame()
	newDF.Rows = len(stats)
	newDF.Cols = 1 + df.NumericCols
	newDF.Columns = append([]string{""}, df.numericColumnNames()...)
	newDF.ColTypes = make([]DataType, newDF.Cols)
	newDF.ColTypes[0] = TypeString
	newDF.StringCols = 1
	newDF.NumericCols = df.NumericCols
	newDF.ColIndex = make(map[string]int)
	for i, col := range newDF.Columns {
		newDF.ColIndex[col] = i
	}

	newDF.StringData = [][]string{stats}
	if df.NumericCols > 0 {
		data := make([]float64, len(stats)*df.NumericCols)
		for j := 0; j < df.NumericCols; j++ {
			colData := df.NumericData.ColView(j)
			vals := make([]float64, df.Rows)
			for i := 0; i < df.Rows; i++ {
				vals[i] = colData.AtVec(i)
			}
			sort.Float64s(vals)
			data[0*df.NumericCols+j] = float64(df.Rows)
			data[1*df.NumericCols+j] = stat.Mean(vals, nil)
			data[2*df.NumericCols+j] = stat.StdDev(vals, nil)
			data[3*df.NumericCols+j] = mat.Min(colData)
			data[4*df.NumericCols+j] = stat.Quantile(0.25, stat.Empirical, vals, nil)
			data[5*df.NumericCols+j] = stat.Quantile(0.50, stat.Empirical, vals, nil)
			data[6*df.NumericCols+j] = stat.Quantile(0.75, stat.Empirical, vals, nil)
			data[7*df.NumericCols+j] = mat.Max(colData)
		}
		newDF.NumericData = mat.NewDense(len(stats), df.NumericCols, data)
	}
	return newDF
}

// numericColumnNames returns names of numeric columns
func (df *DataFrame) numericColumnNames() []string {
	var names []string
	for i, t := range df.ColTypes {
		if t == TypeInt || t == TypeFloat {
			names = append(names, df.Columns[i])
		}
	}
	return names
}

func (df *DataFrame) AddSeries(name string, data interface{}) error {
	df.mu.Lock()
	defer df.mu.Unlock()

	switch v := data.(type) {
	case []float64:
		if len(v) != df.Rows {
			return fmt.Errorf("length of numeric data (%d) does not match DataFrame rows (%d)", len(v), df.Rows)
		}
		if _, exists := df.ColIndex[name]; exists {
			nIdx := 0
			for i, col := range df.Columns {
				if col == name && df.ColTypes[i] != TypeString && df.ColTypes[i] != TypeTime {
					for j := 0; j < df.Rows; j++ {
						df.NumericData.Set(j, nIdx, v[j])
					}
					return nil
				}
				if df.ColTypes[i] != TypeString && df.ColTypes[i] != TypeTime {
					nIdx++
				}
			}
		}
		df.Columns = append(df.Columns, name)
		df.ColTypes = append(df.ColTypes, TypeFloat)
		df.ColIndex[name] = df.Cols
		df.Cols++
		df.NumericCols++
		if df.NumericData == nil {
			df.NumericData = mat.NewDense(df.Rows, 1, v)
		} else {
			newData := make([]float64, df.Rows*df.NumericCols)
			for i := 0; i < df.Rows; i++ {
				for j := 0; j < df.NumericCols-1; j++ {
					newData[i*df.NumericCols+j] = df.NumericData.At(i, j)
				}
				newData[i*df.NumericCols+(df.NumericCols-1)] = v[i]
			}
			df.NumericData = mat.NewDense(df.Rows, df.NumericCols, newData)
		}
	case []time.Time:
		if len(v) != df.Rows {
			return fmt.Errorf("length of time data (%d) does not match DataFrame rows (%d)", len(v), df.Rows)
		}
		df.Columns = append(df.Columns, name)
		df.ColTypes = append(df.ColTypes, TypeTime)
		df.ColIndex[name] = df.Cols
		df.Cols++
		df.TimeCols++
		newTimeData := make([][]time.Time, df.TimeCols)
		for i := 0; i < df.TimeCols-1; i++ {
			newTimeData[i] = df.TimeData[i]
		}
		newTimeData[df.TimeCols-1] = v
		df.TimeData = newTimeData
	default:
		return fmt.Errorf("unsupported series type: %T", data)
	}
	return nil
}

// Join performs a join with another DataFrame
func (df *DataFrame) Join(other *DataFrame, joinType, leftOn, rightOn string) (*DataFrame, error) {

	leftIdx := df.ColIndex[leftOn]
	rightIdx := other.ColIndex[rightOn]

	newDF := NewDataFrame()
	var indicesLeft, indicesRight []int

	if df.ColTypes[leftIdx] == TypeString && other.ColTypes[rightIdx] == TypeString {
		lSIdx := 0
		for i := 0; i < leftIdx; i++ {
			if df.ColTypes[i] == TypeString {
				lSIdx++
			}
		}
		rSIdx := 0
		for i := 0; i < rightIdx; i++ {
			if other.ColTypes[i] == TypeString {
				rSIdx++
			}
		}
		rightMap := make(map[string][]int)
		for i, val := range other.StringData[rSIdx] {
			rightMap[val] = append(rightMap[val], i)
		}
		for i, val := range df.StringData[lSIdx] {
			if rows, ok := rightMap[val]; ok {
				for _, r := range rows {
					indicesLeft = append(indicesLeft, i)
					indicesRight = append(indicesRight, r)
				}
			} else if joinType == "left" {
				indicesLeft = append(indicesLeft, i)
				indicesRight = append(indicesRight, -1)
			}
		}
	} else if df.ColTypes[leftIdx] == TypeTime && other.ColTypes[rightIdx] == TypeTime {
		lTIdx := 0
		for i := 0; i < leftIdx; i++ {
			if df.ColTypes[i] == TypeTime {
				lTIdx++
			}
		}
		rTIdx := 0
		for i := 0; i < rightIdx; i++ {
			if other.ColTypes[i] == TypeTime {
				rTIdx++
			}
		}
		rightMap := make(map[string][]int)
		for i, val := range other.TimeData[rTIdx] {
			key := val.Format("2006-01-02")
			rightMap[key] = append(rightMap[key], i)
		}
		for i, val := range df.TimeData[lTIdx] {
			key := val.Format("2006-01-02")
			if rows, ok := rightMap[key]; ok {
				for _, r := range rows {
					indicesLeft = append(indicesLeft, i)
					indicesRight = append(indicesRight, r)
				}
			} else if joinType == "left" {
				indicesLeft = append(indicesLeft, i)
				indicesRight = append(indicesRight, -1)
			}
		}
	} else {
		return nil, fmt.Errorf("join columns must be both strings or both times")
	}

	newDF.Rows = len(indicesLeft)
	newDF.Cols = df.Cols + other.Cols - 1
	newDF.Columns = append([]string{}, df.Columns...)
	newDF.ColTypes = append([]DataType{}, df.ColTypes...)
	newDF.ColIndex = make(map[string]int)
	for i, col := range df.Columns {
		newDF.ColIndex[col] = i
	}

	newNumericCols := df.NumericCols
	newStringCols := df.StringCols
	newTimeCols := df.TimeCols
	for i, col := range other.Columns {
		if col != rightOn {
			newName := col
			counter := 1
			for newDF.ColIndex[newName] != 0 {
				newName = fmt.Sprintf("%s_%d", col, counter)
				counter++
			}
			newDF.Columns = append(newDF.Columns, newName)
			newDF.ColTypes = append(newDF.ColTypes, other.ColTypes[i])
			newDF.ColIndex[newName] = len(newDF.Columns) - 1
			switch other.ColTypes[i] {
			case TypeString:
				newStringCols++
			case TypeTime:
				newTimeCols++
			default:
				newNumericCols++
			}
		}
	}
	newDF.NumericCols = newNumericCols
	newDF.StringCols = newStringCols
	newDF.TimeCols = newTimeCols

	if df.NumericCols > 0 {
		newNumeric := make([]float64, len(indicesLeft)*df.NumericCols)
		for i, idx := range indicesLeft {
			for j := 0; j < df.NumericCols; j++ {
				newNumeric[i*df.NumericCols+j] = df.NumericData.At(idx, j)
			}
		}
		newDF.NumericData = mat.NewDense(len(indicesLeft), df.NumericCols, newNumeric)
	}
	if df.StringCols > 0 {
		newDF.StringData = make([][]string, df.StringCols)
		for i := 0; i < df.StringCols; i++ {
			newDF.StringData[i] = make([]string, len(indicesLeft))
			for j, idx := range indicesLeft {
				newDF.StringData[i][j] = df.StringData[i][idx]
			}
		}
	}
	if df.TimeCols > 0 {
		newDF.TimeData = make([][]time.Time, df.TimeCols)
		for i := 0; i < df.TimeCols; i++ {
			newDF.TimeData[i] = make([]time.Time, len(indicesLeft))
			for j, idx := range indicesLeft {
				newDF.TimeData[i][j] = df.TimeData[i][idx]
			}
		}
	}

	for i, col := range other.Columns {
		if col != rightOn {
			switch other.ColTypes[i] {
			case TypeString:
				sIdx := 0
				for j := 0; j < i; j++ {
					if other.ColTypes[j] == TypeString {
						sIdx++
					}
				}
				newStrings := make([]string, len(indicesRight))
				for j, idx := range indicesRight {
					if idx >= 0 {
						newStrings[j] = other.StringData[sIdx][idx]
					} else {
						newStrings[j] = "null"
					}
				}
				newDF.StringData = append(newDF.StringData, newStrings)
			case TypeTime:
				tIdx := 0
				for j := 0; j < i; j++ {
					if other.ColTypes[j] == TypeTime {
						tIdx++
					}
				}
				newTimes := make([]time.Time, len(indicesRight))
				for j, idx := range indicesRight {
					if idx >= 0 {
						newTimes[j] = other.TimeData[tIdx][idx]
					}
				}
				newDF.TimeData = append(newDF.TimeData, newTimes)
			default:
				nIdx := 0
				for j := 0; j < i; j++ {
					if other.ColTypes[j] != TypeString && other.ColTypes[j] != TypeTime {
						nIdx++
					}
				}
				newNumeric := make([]float64, len(indicesRight)*newDF.NumericCols)
				if newDF.NumericData != nil {
					oldData := newDF.NumericData.RawMatrix().Data
					for k := 0; k < len(indicesLeft); k++ {
						for j := 0; j < df.NumericCols; j++ {
							newNumeric[k*newDF.NumericCols+j] = oldData[k*df.NumericCols+j]
						}
					}
				}
				for j, idx := range indicesRight {
					if idx >= 0 {
						newNumeric[j*newDF.NumericCols+df.NumericCols+nIdx] = other.NumericData.At(idx, nIdx)
					} else {
						newNumeric[j*newDF.NumericCols+df.NumericCols+nIdx] = math.NaN()
					}
				}
				newDF.NumericData = mat.NewDense(len(indicesRight), newDF.NumericCols, newNumeric)
			}
		}
	}
	return newDF, nil
}

// Head returns the first n rows
func (df *DataFrame) Head(n int) *DataFrame {

	if n >= df.Rows {
		n = df.Rows
	}
	newDF := NewDataFrame()
	newDF.Rows = n
	newDF.Cols = df.Cols
	newDF.Columns = append([]string{}, df.Columns...)
	newDF.ColTypes = append([]DataType{}, df.ColTypes...)
	newDF.NumericCols = df.NumericCols
	newDF.StringCols = df.StringCols
	newDF.TimeCols = df.TimeCols
	newDF.ColIndex = make(map[string]int)
	for k, v := range df.ColIndex {
		newDF.ColIndex[k] = v
	}

	if df.NumericCols > 0 {
		newNumeric := make([]float64, n*df.NumericCols)
		for i := 0; i < n; i++ {
			for j := 0; j < df.NumericCols; j++ {
				newNumeric[i*df.NumericCols+j] = df.NumericData.At(i, j)
			}
		}
		newDF.NumericData = mat.NewDense(n, df.NumericCols, newNumeric)
	}
	if df.StringCols > 0 {
		newDF.StringData = make([][]string, df.StringCols)
		for i := range newDF.StringData {
			newDF.StringData[i] = df.StringData[i][:n]
		}
	}
	if df.TimeCols > 0 {
		newDF.TimeData = make([][]time.Time, df.TimeCols)
		for i := range newDF.TimeData {
			newDF.TimeData[i] = df.TimeData[i][:n]
		}
	}
	return newDF
}

// Tail returns the last n rows
func (df *DataFrame) Tail(n int) *DataFrame {
	if n >= df.Rows {
		n = df.Rows
	}
	start := df.Rows - n
	newDF := NewDataFrame()
	newDF.Rows = n
	newDF.Cols = df.Cols
	newDF.Columns = append([]string{}, df.Columns...)
	newDF.ColTypes = append([]DataType{}, df.ColTypes...)
	newDF.NumericCols = df.NumericCols
	newDF.StringCols = df.StringCols
	newDF.TimeCols = df.TimeCols
	newDF.ColIndex = make(map[string]int)
	for k, v := range df.ColIndex {
		newDF.ColIndex[k] = v
	}

	if df.NumericCols > 0 {
		newNumeric := make([]float64, n*df.NumericCols)
		for i := 0; i < n; i++ {
			for j := 0; j < df.NumericCols; j++ {
				newNumeric[i*df.NumericCols+j] = df.NumericData.At(start+i, j)
			}
		}
		newDF.NumericData = mat.NewDense(n, df.NumericCols, newNumeric)
	}
	if df.StringCols > 0 {
		newDF.StringData = make([][]string, df.StringCols)
		for i := range newDF.StringData {
			newDF.StringData[i] = df.StringData[i][start:]
		}
	}
	if df.TimeCols > 0 {
		newDF.TimeData = make([][]time.Time, df.TimeCols)
		for i := range newDF.TimeData {
			newDF.TimeData[i] = df.TimeData[i][start:]
		}
	}
	return newDF
}

// RemoveDuplicates removes duplicate rows based on columns
func (df *DataFrame) RemoveDuplicates(columns []string) *DataFrame {

	if len(columns) == 0 {
		columns = df.Columns
	}
	seen := make(map[string]bool)
	indices := make([]int, 0, df.Rows)
	for i := 0; i < df.Rows; i++ {
		keyParts := make([]string, len(columns))
		for j, col := range columns {
			idx := df.ColIndex[col]
			switch df.ColTypes[idx] {
			case TypeString:
				sIdx := 0
				for k := 0; k < idx; k++ {
					if df.ColTypes[k] == TypeString {
						sIdx++
					}
				}
				keyParts[j] = df.StringData[sIdx][i]
			case TypeTime:
				tIdx := 0
				for k := 0; k < idx; k++ {
					if df.ColTypes[k] == TypeTime {
						tIdx++
					}
				}
				keyParts[j] = df.TimeData[tIdx][i].Format("2006-01-02")
			default:
				nIdx := 0
				for k := 0; k < idx; k++ {
					if df.ColTypes[k] != TypeString && df.ColTypes[k] != TypeTime {
						nIdx++
					}
				}
				keyParts[j] = fmt.Sprintf("%v", df.NumericData.At(i, nIdx))
			}
		}
		key := strings.Join(keyParts, "|")
		if !seen[key] {
			seen[key] = true
			indices = append(indices, i)
		}
	}

	newDF := NewDataFrame()
	newDF.Rows = len(indices)
	newDF.Cols = df.Cols
	newDF.Columns = append([]string{}, df.Columns...)
	newDF.ColTypes = append([]DataType{}, df.ColTypes...)
	newDF.NumericCols = df.NumericCols
	newDF.StringCols = df.StringCols
	newDF.TimeCols = df.TimeCols
	newDF.ColIndex = make(map[string]int)
	for k, v := range df.ColIndex {
		newDF.ColIndex[k] = v
	}

	if df.NumericCols > 0 {
		newNumeric := make([]float64, len(indices)*df.NumericCols)
		for i, idx := range indices {
			for j := 0; j < df.NumericCols; j++ {
				newNumeric[i*df.NumericCols+j] = df.NumericData.At(idx, j)
			}
		}
		newDF.NumericData = mat.NewDense(len(indices), df.NumericCols, newNumeric)
	}
	if df.StringCols > 0 {
		newDF.StringData = make([][]string, df.StringCols)
		for i := range newDF.StringData {
			newDF.StringData[i] = make([]string, len(indices))
			for j, idx := range indices {
				newDF.StringData[i][j] = df.StringData[i][idx]
			}
		}
	}
	if df.TimeCols > 0 {
		newDF.TimeData = make([][]time.Time, df.TimeCols)
		for i := range newDF.TimeData {
			newDF.TimeData[i] = make([]time.Time, len(indices))
			for j, idx := range indices {
				newDF.TimeData[i][j] = df.TimeData[i][idx]
			}
		}
	}
	return newDF
}

// Rename renames a column
func (df *DataFrame) Rename(oldName, newName string) error {

	if idx, ok := df.ColIndex[oldName]; ok {
		df.Columns[idx] = newName
		delete(df.ColIndex, oldName)
		df.ColIndex[newName] = idx
		return nil
	}
	return fmt.Errorf("column '%s' not found", oldName)
}

// DropNA drops rows with missing values
func (df *DataFrame) DropNA() *DataFrame {

	indices := make([]int, 0, df.Rows)
	for i := 0; i < df.Rows; i++ {
		hasNull := false
		for j := 0; j < df.NumericCols; j++ {
			if math.IsNaN(df.NumericData.At(i, j)) {
				hasNull = true
				break
			}
		}
		if !hasNull {
			for _, s := range df.StringData {
				if s[i] == "null" || s[i] == "" {
					hasNull = true
					break
				}
			}
		}
		if !hasNull {
			for _, t := range df.TimeData {
				if t[i].IsZero() {
					hasNull = true
					break
				}
			}
		}
		if !hasNull {
			indices = append(indices, i)
		}
	}

	newDF := NewDataFrame()
	newDF.Rows = len(indices)
	newDF.Cols = df.Cols
	newDF.Columns = append([]string{}, df.Columns...)
	newDF.ColTypes = append([]DataType{}, df.ColTypes...)
	newDF.NumericCols = df.NumericCols
	newDF.StringCols = df.StringCols
	newDF.TimeCols = df.TimeCols
	newDF.ColIndex = make(map[string]int)
	for k, v := range df.ColIndex {
		newDF.ColIndex[k] = v
	}

	if df.NumericCols > 0 {
		newNumeric := make([]float64, len(indices)*df.NumericCols)
		for i, idx := range indices {
			for j := 0; j < df.NumericCols; j++ {
				newNumeric[i*df.NumericCols+j] = df.NumericData.At(idx, j)
			}
		}
		newDF.NumericData = mat.NewDense(len(indices), df.NumericCols, newNumeric)
	}
	if df.StringCols > 0 {
		newDF.StringData = make([][]string, df.StringCols)
		for i := range newDF.StringData {
			newDF.StringData[i] = make([]string, len(indices))
			for j, idx := range indices {
				newDF.StringData[i][j] = df.StringData[i][idx]
			}
		}
	}
	if df.TimeCols > 0 {
		newDF.TimeData = make([][]time.Time, df.TimeCols)
		for i := range newDF.TimeData {
			newDF.TimeData[i] = make([]time.Time, len(indices))
			for j, idx := range indices {
				newDF.TimeData[i][j] = df.TimeData[i][idx]
			}
		}
	}
	return newDF
}

// FillNA fills missing values
func (df *DataFrame) FillNA(values map[string]interface{}) *DataFrame {

	newDF := df.clone()
	for col, val := range values {
		if idx, ok := newDF.ColIndex[col]; ok {
			switch newDF.ColTypes[idx] {
			case TypeString:
				sIdx := 0
				for i := 0; i < idx; i++ {
					if newDF.ColTypes[i] == TypeString {
						sIdx++
					}
				}
				for i := 0; i < newDF.Rows; i++ {
					if newDF.StringData[sIdx][i] == "null" || newDF.StringData[sIdx][i] == "" {
						newDF.StringData[sIdx][i] = fmt.Sprintf("%v", val)
					}
				}
			case TypeTime:
				tIdx := 0
				for i := 0; i < idx; i++ {
					if newDF.ColTypes[i] == TypeTime {
						tIdx++
					}
				}
				var t time.Time
				switch v := val.(type) {
				case time.Time:
					t = v
				case string:
					t, _ = time.Parse("2006-01-02", v)
				}
				for i := 0; i < newDF.Rows; i++ {
					if newDF.TimeData[tIdx][i].IsZero() {
						newDF.TimeData[tIdx][i] = t
					}
				}
			default:
				nIdx := 0
				for i := 0; i < idx; i++ {
					if newDF.ColTypes[i] != TypeString && newDF.ColTypes[i] != TypeTime {
						nIdx++
					}
				}
				f, _ := strconv.ParseFloat(fmt.Sprintf("%v", val), 64)
				for i := 0; i < newDF.Rows; i++ {
					if math.IsNaN(newDF.NumericData.At(i, nIdx)) {
						newDF.NumericData.Set(i, nIdx, f)
					}
				}
			}
		}
	}
	return newDF
}

// AsType converts a column to a specified type
func (df *DataFrame) AsType(column string, typeName DataType) error {

	idx, ok := df.ColIndex[column]
	if !ok {
		return fmt.Errorf("column '%s' not found", column)
	}

	switch df.ColTypes[idx] {
	case TypeString:
		sIdx := 0
		for i := 0; i < idx; i++ {
			if df.ColTypes[i] == TypeString {
				sIdx++
			}
		}
		switch typeName {
		case TypeTime:
			newTimes := make([]time.Time, df.Rows)
			for i, val := range df.StringData[sIdx] {
				if t, err := time.Parse("2006-01-02", val); err == nil {
					newTimes[i] = t
				}
			}
			df.TimeData = append(df.TimeData, newTimes)
			df.TimeCols++
			df.ColTypes[idx] = TypeTime
			df.StringData = append(df.StringData[:sIdx], df.StringData[sIdx+1:]...)
			df.StringCols--
		case TypeInt, TypeFloat:
			newFloats := make([]float64, df.Rows)
			for i, val := range df.StringData[sIdx] {
				f, err := strconv.ParseFloat(val, 64)
				if err != nil {
					newFloats[i] = math.NaN()
				} else {
					newFloats[i] = f
				}
			}
			if df.NumericData == nil {
				df.NumericData = mat.NewDense(df.Rows, 1, newFloats)
			} else {
				newData := make([]float64, df.Rows*(df.NumericCols+1))
				for i := 0; i < df.Rows; i++ {
					for j := 0; j < df.NumericCols; j++ {
						newData[i*(df.NumericCols+1)+j] = df.NumericData.At(i, j)
					}
					newData[i*(df.NumericCols+1)+df.NumericCols] = newFloats[i]
				}
				df.NumericData = mat.NewDense(df.Rows, df.NumericCols+1, newData)
			}
			df.NumericCols++
			if typeName == TypeInt {
				nIdx := df.NumericCols - 1
				for i := 0; i < df.Rows; i++ {
					v := df.NumericData.At(i, nIdx)
					if !math.IsNaN(v) {
						df.NumericData.Set(i, nIdx, float64(int64(v)))
					}
				}
			}
			df.ColTypes[idx] = typeName
			df.StringData = append(df.StringData[:sIdx], df.StringData[sIdx+1:]...)
			df.StringCols--
		}
	case TypeTime:
		if typeName != TypeString {
			return fmt.Errorf("can only convert time to string")
		}
		tIdx := 0
		for i := 0; i < idx; i++ {
			if df.ColTypes[i] == TypeTime {
				tIdx++
			}
		}
		newStrings := make([]string, df.Rows)
		for i, t := range df.TimeData[tIdx] {
			if t.IsZero() {
				newStrings[i] = "null"
			} else {
				newStrings[i] = t.Format("2006-01-02")
			}
		}
		df.StringData = append(df.StringData, newStrings)
		df.StringCols++
		df.ColTypes[idx] = TypeString
		df.TimeData = append(df.TimeData[:tIdx], df.TimeData[tIdx+1:]...)
		df.TimeCols--
	default:
		nIdx := 0
		for i := 0; i < idx; i++ {
			if df.ColTypes[i] != TypeString && df.ColTypes[i] != TypeTime {
				nIdx++
			}
		}
		if typeName == TypeInt {
			for i := 0; i < df.Rows; i++ {
				v := df.NumericData.At(i, nIdx)
				if !math.IsNaN(v) {
					df.NumericData.Set(i, nIdx, float64(int64(v)))
				}
			}
			df.ColTypes[idx] = TypeInt
		}
	}
	return nil
}

// CumSum calculates cumulative sum using Gonum
func (df *DataFrame) CumSum() *DataFrame {

	newDF := df.clone()
	if df.NumericCols > 0 {
		for j := 0; j < df.NumericCols; j++ {
			var sum float64
			for i := 0; i < df.Rows; i++ {
				v := df.NumericData.At(i, j)
				if !math.IsNaN(v) {
					sum += v
				}
				newDF.NumericData.Set(i, j, sum)
			}
		}
	}
	return newDF
}

// Drop drops specified columns
func (df *DataFrame) Drop(columns ...string) *DataFrame {

	keep := make(map[string]bool)
	for _, col := range df.Columns {
		keep[col] = true
	}
	for _, col := range columns {
		delete(keep, col)
	}
	newDF := NewDataFrame()
	newDF.Rows = df.Rows
	for i, col := range df.Columns {
		if keep[col] {
			newDF.Columns = append(newDF.Columns, col)
			newDF.ColTypes = append(newDF.ColTypes, df.ColTypes[i])
			newDF.ColIndex[col] = len(newDF.Columns) - 1
			switch df.ColTypes[i] {
			case TypeString:
				newDF.StringCols++
			case TypeTime:
				newDF.TimeCols++
			default:
				newDF.NumericCols++
			}
		}
	}
	newDF.Cols = len(newDF.Columns)

	if newDF.NumericCols > 0 {
		newNumeric := make([]float64, df.Rows*newDF.NumericCols)
		nIdx := 0
		for i, col := range df.Columns {
			if keep[col] && df.ColTypes[i] != TypeString && df.ColTypes[i] != TypeTime {
				nOldIdx := 0
				for k := 0; k < i; k++ {
					if df.ColTypes[k] != TypeString && df.ColTypes[k] != TypeTime {
						nOldIdx++
					}
				}
				for j := 0; j < df.Rows; j++ {
					newNumeric[j*newDF.NumericCols+nIdx] = df.NumericData.At(j, nOldIdx)
				}
				nIdx++
			}
		}
		newDF.NumericData = mat.NewDense(df.Rows, newDF.NumericCols, newNumeric)
	}
	if newDF.StringCols > 0 {
		newDF.StringData = make([][]string, newDF.StringCols)
		sIdx := 0
		for i, col := range df.Columns {
			if keep[col] && df.ColTypes[i] == TypeString {
				sOldIdx := 0
				for k := 0; k < i; k++ {
					if df.ColTypes[k] == TypeString {
						sOldIdx++
					}
				}
				newDF.StringData[sIdx] = append([]string{}, df.StringData[sOldIdx]...)
				sIdx++
			}
		}
	}
	if newDF.TimeCols > 0 {
		newDF.TimeData = make([][]time.Time, newDF.TimeCols)
		tIdx := 0
		for i, col := range df.Columns {
			if keep[col] && df.ColTypes[i] == TypeTime {
				tOldIdx := 0
				for k := 0; k < i; k++ {
					if df.ColTypes[k] == TypeTime {
						tOldIdx++
					}
				}
				newDF.TimeData[tIdx] = append([]time.Time{}, df.TimeData[tOldIdx]...)
				tIdx++
			}
		}
	}
	return newDF
}

// Select selects specific columns
func (df *DataFrame) Select(columns ...string) *DataFrame {

	newDF := NewDataFrame()
	newDF.Rows = df.Rows
	for _, col := range columns {
		if idx, ok := df.ColIndex[col]; ok {
			newDF.Columns = append(newDF.Columns, col)
			newDF.ColTypes = append(newDF.ColTypes, df.ColTypes[idx])
			newDF.ColIndex[col] = len(newDF.Columns) - 1
			switch df.ColTypes[idx] {
			case TypeString:
				newDF.StringCols++
			case TypeTime:
				newDF.TimeCols++
			default:
				newDF.NumericCols++
			}
		}
	}
	newDF.Cols = len(newDF.Columns)

	if newDF.NumericCols > 0 {
		newNumeric := make([]float64, df.Rows*newDF.NumericCols)
		nIdx := 0
		for _, col := range columns {
			if idx, ok := df.ColIndex[col]; ok && df.ColTypes[idx] != TypeString && df.ColTypes[idx] != TypeTime {
				nOldIdx := 0
				for k := 0; k < idx; k++ {
					if df.ColTypes[k] != TypeString && df.ColTypes[k] != TypeTime {
						nOldIdx++
					}
				}
				for j := 0; j < df.Rows; j++ {
					newNumeric[j*newDF.NumericCols+nIdx] = df.NumericData.At(j, nOldIdx)
				}
				nIdx++
			}
		}
		newDF.NumericData = mat.NewDense(df.Rows, newDF.NumericCols, newNumeric)
	}
	if newDF.StringCols > 0 {
		newDF.StringData = make([][]string, newDF.StringCols)
		sIdx := 0
		for _, col := range columns {
			if idx, ok := df.ColIndex[col]; ok && df.ColTypes[idx] == TypeString {
				sOldIdx := 0
				for k := 0; k < idx; k++ {
					if df.ColTypes[k] == TypeString {
						sOldIdx++
					}
				}
				newDF.StringData[sIdx] = append([]string{}, df.StringData[sOldIdx]...)
				sIdx++
			}
		}
	}
	if newDF.TimeCols > 0 {
		newDF.TimeData = make([][]time.Time, newDF.TimeCols)
		tIdx := 0
		for _, col := range columns {
			if idx, ok := df.ColIndex[col]; ok && df.ColTypes[idx] == TypeTime {
				tOldIdx := 0
				for k := 0; k < idx; k++ {
					if df.ColTypes[k] == TypeTime {
						tOldIdx++
					}
				}
				newDF.TimeData[tIdx] = append([]time.Time{}, df.TimeData[tOldIdx]...)
				tIdx++
			}
		}
	}
	return newDF
}

// Concat concatenates two DataFrames
func (df *DataFrame) Concat(other *DataFrame) (*DataFrame, error) {

	newDF := NewDataFrame()
	newDF.Rows = df.Rows + other.Rows

	colOrder := make([]string, 0, df.Cols+other.Cols)
	colMap := make(map[string]bool)
	for _, col := range df.Columns {
		if !colMap[col] {
			colOrder = append(colOrder, col)
			colMap[col] = true
		}
	}
	for _, col := range other.Columns {
		if !colMap[col] {
			colOrder = append(colOrder, col)
			colMap[col] = true
		}
	}
	newDF.Columns = colOrder
	newDF.Cols = len(newDF.Columns)
	newDF.ColTypes = make([]DataType, newDF.Cols)
	newDF.ColIndex = make(map[string]int)

	for i, col := range newDF.Columns {
		newDF.ColIndex[col] = i
		if dfIdx, inDF := df.ColIndex[col]; inDF {
			newDF.ColTypes[i] = df.ColTypes[dfIdx]
		} else if oIdx, inOther := other.ColIndex[col]; inOther {
			newDF.ColTypes[i] = other.ColTypes[oIdx]
		}
		switch newDF.ColTypes[i] {
		case TypeString:
			newDF.StringCols++
		case TypeTime:
			newDF.TimeCols++
		default:
			newDF.NumericCols++
		}
	}

	if newDF.NumericCols > 0 {
		newNumeric := make([]float64, newDF.Rows*newDF.NumericCols)
		for i, col := range newDF.Columns {
			if newDF.ColTypes[i] != TypeString && newDF.ColTypes[i] != TypeTime {
				nIdx := 0
				for k := 0; k < i; k++ {
					if newDF.ColTypes[k] != TypeString && newDF.ColTypes[k] != TypeTime {
						nIdx++
					}
				}
				if dfIdx, ok := df.ColIndex[col]; ok && df.ColTypes[dfIdx] != TypeString && df.ColTypes[dfIdx] != TypeTime {
					dNIdx := 0
					for k := 0; k < dfIdx; k++ {
						if df.ColTypes[k] != TypeString && df.ColTypes[k] != TypeTime {
							dNIdx++
						}
					}
					for j := 0; j < df.Rows; j++ {
						newNumeric[j*newDF.NumericCols+nIdx] = df.NumericData.At(j, dNIdx)
					}
				} else {
					for j := 0; j < df.Rows; j++ {
						newNumeric[j*newDF.NumericCols+nIdx] = math.NaN()
					}
				}
				if oIdx, ok := other.ColIndex[col]; ok && other.ColTypes[oIdx] != TypeString && other.ColTypes[oIdx] != TypeTime {
					oNIdx := 0
					for k := 0; k < oIdx; k++ {
						if other.ColTypes[k] != TypeString && other.ColTypes[k] != TypeTime {
							oNIdx++
						}
					}
					for j := 0; j < other.Rows; j++ {
						newNumeric[(df.Rows+j)*newDF.NumericCols+nIdx] = other.NumericData.At(j, oNIdx)
					}
				} else {
					for j := 0; j < other.Rows; j++ {
						newNumeric[(df.Rows+j)*newDF.NumericCols+nIdx] = math.NaN()
					}
				}
			}
		}
		newDF.NumericData = mat.NewDense(newDF.Rows, newDF.NumericCols, newNumeric)
	}

	if newDF.StringCols > 0 {
		newDF.StringData = make([][]string, newDF.StringCols)
		for i := range newDF.StringData {
			newDF.StringData[i] = make([]string, newDF.Rows)
		}
		for i, col := range newDF.Columns {
			if newDF.ColTypes[i] == TypeString {
				sIdx := 0
				for k := 0; k < i; k++ {
					if newDF.ColTypes[k] == TypeString {
						sIdx++
					}
				}
				if dfIdx, ok := df.ColIndex[col]; ok && df.ColTypes[dfIdx] == TypeString {
					dSIdx := 0
					for k := 0; k < dfIdx; k++ {
						if df.ColTypes[k] == TypeString {
							dSIdx++
						}
					}
					for j := 0; j < df.Rows; j++ {
						newDF.StringData[sIdx][j] = df.StringData[dSIdx][j]
					}
				} else {
					for j := 0; j < df.Rows; j++ {
						newDF.StringData[sIdx][j] = "null"
					}
				}
				if oIdx, ok := other.ColIndex[col]; ok && other.ColTypes[oIdx] == TypeString {
					oSIdx := 0
					for k := 0; k < oIdx; k++ {
						if other.ColTypes[k] == TypeString {
							oSIdx++
						}
					}
					for j := 0; j < other.Rows; j++ {
						newDF.StringData[sIdx][df.Rows+j] = other.StringData[oSIdx][j]
					}
				} else {
					for j := 0; j < other.Rows; j++ {
						newDF.StringData[sIdx][df.Rows+j] = "null"
					}
				}
			}
		}
	}

	if newDF.TimeCols > 0 {
		newDF.TimeData = make([][]time.Time, newDF.TimeCols)
		for i := range newDF.TimeData {
			newDF.TimeData[i] = make([]time.Time, newDF.Rows)
		}
		for i, col := range newDF.Columns {
			if newDF.ColTypes[i] == TypeTime {
				tIdx := 0
				for k := 0; k < i; k++ {
					if newDF.ColTypes[k] == TypeTime {
						tIdx++
					}
				}
				if dfIdx, ok := df.ColIndex[col]; ok && df.ColTypes[dfIdx] == TypeTime {
					dTIdx := 0
					for k := 0; k < dfIdx; k++ {
						if df.ColTypes[k] == TypeTime {
							dTIdx++
						}
					}
					for j := 0; j < df.Rows; j++ {
						newDF.TimeData[tIdx][j] = df.TimeData[dTIdx][j]
					}
				}
				if oIdx, ok := other.ColIndex[col]; ok && other.ColTypes[oIdx] == TypeTime {
					oTIdx := 0
					for k := 0; k < oIdx; k++ {
						if other.ColTypes[k] == TypeTime {
							oTIdx++
						}
					}
					for j := 0; j < other.Rows; j++ {
						newDF.TimeData[tIdx][df.Rows+j] = other.TimeData[oTIdx][j]
					}
				}
			}
		}
	}
	return newDF, nil
}

// clone creates a deep copy of the DataFrame
func (df *DataFrame) clone() *DataFrame {
	newDF := NewDataFrame()
	newDF.Rows = df.Rows
	newDF.Cols = df.Cols
	newDF.Columns = append([]string{}, df.Columns...)
	newDF.ColTypes = append([]DataType{}, df.ColTypes...)
	newDF.NumericCols = df.NumericCols
	newDF.StringCols = df.StringCols
	newDF.TimeCols = df.TimeCols
	newDF.ColIndex = make(map[string]int)
	for k, v := range df.ColIndex {
		newDF.ColIndex[k] = v
	}
	if df.NumericCols > 0 {
		data := make([]float64, df.Rows*df.NumericCols)
		copy(data, df.NumericData.RawMatrix().Data)
		newDF.NumericData = mat.NewDense(df.Rows, df.NumericCols, data)
	}
	if df.StringCols > 0 {
		newDF.StringData = make([][]string, df.StringCols)
		for i := range newDF.StringData {
			newDF.StringData[i] = append([]string{}, df.StringData[i]...)
		}
	}
	if df.TimeCols > 0 {
		newDF.TimeData = make([][]time.Time, df.TimeCols)
		for i := range newDF.TimeData {
			newDF.TimeData[i] = append([]time.Time{}, df.TimeData[i]...)
		}
	}
	return newDF
}

func ReadSQLToDataFrame(driver, connString, query string) (*DataFrame, error) {

	db, err := sql.Open(driver, connString)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %v", err)
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting columns: %v", err)
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("error getting column types: %v", err)
	}

	df := NewDataFrame()
	df.Columns = columns
	df.Cols = len(columns)
	df.ColTypes = make([]DataType, df.Cols)
	df.ColIndex = make(map[string]int)

	for i, ct := range colTypes {
		switch ct.DatabaseTypeName() {
		case "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT":
			df.ColTypes[i] = TypeInt
			df.NumericCols++
		case "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL":
			df.ColTypes[i] = TypeFloat
			df.NumericCols++
		case "DATETIME", "TIMESTAMP", "DATE", "TIME":
			df.ColTypes[i] = TypeTime
			df.TimeCols++
		default:
			df.ColTypes[i] = TypeString
			df.StringCols++
		}
		df.ColIndex[columns[i]] = i
	}

	var allRows [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			switch df.ColTypes[i] {
			case TypeInt:
				var v int64
				valuePtrs[i] = &v
			case TypeFloat:
				var v float64
				valuePtrs[i] = &v
			case TypeTime:
				var v time.Time
				valuePtrs[i] = &v
			default:
				var v string
				valuePtrs[i] = &v
			}
		}
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}
		allRows = append(allRows, valuePtrs)
	}

	df.Rows = len(allRows)
	if df.Rows == 0 {
		return df, nil
	}

	if df.NumericCols > 0 {
		df.NumericData = mat.NewDense(df.Rows, df.NumericCols, make([]float64, df.Rows*df.NumericCols))
	}
	if df.StringCols > 0 {
		df.StringData = make([][]string, df.StringCols)
		for i := range df.StringData {
			df.StringData[i] = make([]string, df.Rows)
		}
	}
	if df.TimeCols > 0 {
		df.TimeData = make([][]time.Time, df.TimeCols)
		for i := range df.TimeData {
			df.TimeData[i] = make([]time.Time, df.Rows)
		}
	}

	nIdx, sIdx, tIdx := 0, 0, 0
	for rowIdx, row := range allRows {
		nIdx, sIdx, tIdx = 0, 0, 0
		for colIdx, val := range row {
			switch df.ColTypes[colIdx] {
			case TypeInt:
				if v, ok := val.(*int64); ok && v != nil {
					df.NumericData.Set(rowIdx, nIdx, float64(*v))
				} else {
					df.NumericData.Set(rowIdx, nIdx, math.NaN())
				}
				nIdx++
			case TypeFloat:
				if v, ok := val.(*float64); ok && v != nil {
					df.NumericData.Set(rowIdx, nIdx, *v)
				} else {
					df.NumericData.Set(rowIdx, nIdx, math.NaN())
				}
				nIdx++
			case TypeString:
				if v, ok := val.(*string); ok && v != nil {
					df.StringData[sIdx][rowIdx] = *v
				} else {
					df.StringData[sIdx][rowIdx] = "null"
				}
				sIdx++
			case TypeTime:
				if v, ok := val.(*time.Time); ok && v != nil {
					df.TimeData[tIdx][rowIdx] = *v
				}
				tIdx++
			}
		}
	}

	return df, nil
}

// ReadJSONBytesToDataFrame reads JSON bytes into a DataFrame
func ReadJSONBytesToDataFrame(data []byte) (*DataFrame, error) {

	var jsonData []map[string]interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, err
	}
	return NewDataFrameFromInterface(jsonData)
}

// ReadParquetFileToDataFrame reads a Parquet file into a DataFrame using reflection
func ReadParquetFileToDataFrame(filename string) (*DataFrame, error) {

	fr, err := local.NewLocalFileReader(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening parquet file: %v", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return nil, fmt.Errorf("error creating parquet reader: %v", err)
	}
	defer pr.ReadStop()

	numRows := int(pr.GetNumRows())
	schemaElements := pr.Footer.GetSchema()

	df := NewDataFrame()
	df.Rows = numRows

	for i, elem := range schemaElements[1:] { // Skip root element
		colName := elem.GetName()
		df.Columns = append(df.Columns, colName)
		switch *elem.Type {
		case parquet.Type_INT32, parquet.Type_INT64:
			df.ColTypes = append(df.ColTypes, TypeInt)
			df.NumericCols++
		case parquet.Type_FLOAT, parquet.Type_DOUBLE:
			df.ColTypes = append(df.ColTypes, TypeFloat)
			df.NumericCols++
		case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
			df.ColTypes = append(df.ColTypes, TypeString)
			df.StringCols++
		case parquet.Type_INT96: // Treat as timestamp
			df.ColTypes = append(df.ColTypes, TypeTime)
			df.TimeCols++
		default:
			df.ColTypes = append(df.ColTypes, TypeString)
			df.StringCols++
		}
		df.ColIndex[colName] = i
	}
	df.Cols = len(df.Columns)

	if numRows > 0 {
		data, err := pr.ReadByNumber(numRows)
		if err != nil {
			return nil, fmt.Errorf("error reading parquet data: %v", err)
		}

		numericData := make([]float64, numRows*df.NumericCols)
		stringData := make([][]string, df.StringCols)
		timeData := make([][]time.Time, df.TimeCols)
		for i := range stringData {
			stringData[i] = make([]string, numRows)
		}
		for i := range timeData {
			timeData[i] = make([]time.Time, numRows)
		}

		type fieldInfo struct {
			index int
			kind  reflect.Kind
		}
		fieldMap := make(map[string]fieldInfo)
		if len(data) > 0 {
			val := reflect.ValueOf(data[0])
			if val.Kind() == reflect.Struct {
				for i := 0; i < val.NumField(); i++ {
					fieldName := val.Type().Field(i).Name
					for colName := range df.ColIndex {
						if strings.EqualFold(fieldName, colName) {
							fieldMap[colName] = fieldInfo{
								index: i,
								kind:  val.Field(i).Kind(),
							}
							break
						}
					}
				}
			}
		}

		for rowIdx, row := range data {
			val := reflect.ValueOf(row)
			if val.Kind() != reflect.Struct {
				return nil, fmt.Errorf("expected struct, got %v", val.Kind())
			}

			nIdx, sIdx, tIdx := 0, 0, 0
			for colIdx, colName := range df.Columns {
				fInfo, ok := fieldMap[colName]
				if !ok {
					switch df.ColTypes[colIdx] {
					case TypeInt, TypeFloat:
						numericData[rowIdx*df.NumericCols+nIdx] = math.NaN()
						nIdx++
					case TypeString:
						stringData[sIdx][rowIdx] = "null"
						sIdx++
					case TypeTime:
						timeData[tIdx][rowIdx] = time.Time{}
						tIdx++
					}
					continue
				}

				field := val.Field(fInfo.index)
				if field.Kind() == reflect.Ptr && field.IsNil() {
					switch df.ColTypes[colIdx] {
					case TypeInt, TypeFloat:
						numericData[rowIdx*df.NumericCols+nIdx] = math.NaN()
						nIdx++
					case TypeString:
						stringData[sIdx][rowIdx] = "null"
						sIdx++
					case TypeTime:
						timeData[tIdx][rowIdx] = time.Time{}
						tIdx++
					}
					continue
				}

				if field.Kind() == reflect.Ptr {
					field = field.Elem()
				}

				switch df.ColTypes[colIdx] {
				case TypeInt:
					if field.Kind() == reflect.Int64 || field.Kind() == reflect.Int32 {
						numericData[rowIdx*df.NumericCols+nIdx] = float64(field.Int())
					} else {
						numericData[rowIdx*df.NumericCols+nIdx] = math.NaN()
					}
					nIdx++
				case TypeFloat:
					if field.Kind() == reflect.Float64 || field.Kind() == reflect.Float32 {
						numericData[rowIdx*df.NumericCols+nIdx] = field.Float()
					} else {
						numericData[rowIdx*df.NumericCols+nIdx] = math.NaN()
					}
					nIdx++
				case TypeString:
					if field.Kind() == reflect.String {
						stringData[sIdx][rowIdx] = field.String()
					} else {
						stringData[sIdx][rowIdx] = fmt.Sprintf("%v", field.Interface())
					}
					sIdx++
				case TypeTime:
					if field.Kind() == reflect.String {
						if t, err := time.Parse(time.RFC3339, field.String()); err == nil {
							timeData[tIdx][rowIdx] = t
						} else if t, err := time.Parse("2006-01-02", field.String()); err == nil {
							timeData[tIdx][rowIdx] = t
						}
					} else if field.Kind() == reflect.Int64 {
						timeData[tIdx][rowIdx] = time.UnixMilli(field.Int())
					}
					tIdx++
				}
			}
		}

		if df.NumericCols > 0 {
			df.NumericData = mat.NewDense(numRows, df.NumericCols, numericData)
		}
		if df.StringCols > 0 {
			df.StringData = stringData
		}
		if df.TimeCols > 0 {
			df.TimeData = timeData
		}
	}

	return df, nil
}

// WriteToCSV writes the DataFrame to a CSV file
func (df *DataFrame) WriteToCSV(filename string) error {

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write(df.Columns); err != nil {
		return fmt.Errorf("error writing headers: %v", err)
	}

	row := make([]string, df.Cols)
	for i := 0; i < df.Rows; i++ {
		for j := 0; j < df.Cols; j++ {
			row[j] = df.getValue(i, j)
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("error writing row %d: %v", i, err)
		}
	}

	return nil
}

// WriteToJSON writes the DataFrame to a JSON file
func (df *DataFrame) WriteToJSON(filename string) error {

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating file: %v", err)
	}
	defer file.Close()

	data := make([]map[string]interface{}, df.Rows)
	for i := 0; i < df.Rows; i++ {
		data[i] = make(map[string]interface{}, df.Cols)
		for j, col := range df.Columns {
			switch df.ColTypes[j] {
			case TypeString:
				sIdx := 0
				for k := 0; k < j; k++ {
					if df.ColTypes[k] == TypeString {
						sIdx++
					}
				}
				data[i][col] = df.StringData[sIdx][i]
			case TypeTime:
				tIdx := 0
				for k := 0; k < j; k++ {
					if df.ColTypes[k] == TypeTime {
						tIdx++
					}
				}
				if df.TimeData[tIdx][i].IsZero() {
					data[i][col] = nil
				} else {
					data[i][col] = df.TimeData[tIdx][i].Format(time.RFC3339)
				}
			default:
				nIdx := 0
				for k := 0; k < j; k++ {
					if df.ColTypes[k] != TypeString && df.ColTypes[k] != TypeTime {
						nIdx++
					}
				}
				val := df.NumericData.At(i, nIdx)
				if math.IsNaN(val) {
					data[i][col] = nil
				} else if df.ColTypes[j] == TypeInt {
					data[i][col] = int64(val)
				} else {
					data[i][col] = val
				}
			}
		}
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		return fmt.Errorf("error encoding JSON: %v", err)
	}

	return nil
}

// WriteToParquet writes the DataFrame to a Parquet file
func (df *DataFrame) WriteToParquet(filename string) error {

	fw, err := local.NewLocalFileWriter(filename)
	if err != nil {
		return fmt.Errorf("error creating parquet file: %v", err)
	}
	defer fw.Close()

	// Define a struct type with Parquet tags for the schema
	fields := make([]reflect.StructField, df.Cols)
	for i, col := range df.Columns {
		var typ reflect.Type
		var tag string
		switch df.ColTypes[i] {
		case TypeInt:
			typ = reflect.TypeOf(int64(0))
			tag = fmt.Sprintf(`parquet:"name=%s, type=INT64"`, col)
		case TypeFloat:
			typ = reflect.TypeOf(float64(0))
			tag = fmt.Sprintf(`parquet:"name=%s, type=DOUBLE"`, col)
		case TypeString:
			typ = reflect.TypeOf("")
			tag = fmt.Sprintf(`parquet:"name=%s, type=BYTE_ARRAY, convertedtype=UTF8"`, col)
		case TypeTime:
			typ = reflect.TypeOf(time.Time{})
			tag = fmt.Sprintf(`parquet:"name=%s, type=INT96"`, col)
		}
		fields[i] = reflect.StructField{
			Name: strings.Title(col),
			Type: typ,
			Tag:  reflect.StructTag(tag),
		}
	}
	schemaType := reflect.StructOf(fields)

	// Create Parquet writer with the schema
	pw, err := writer.NewParquetWriter(fw, schemaType, int64(runtime.NumCPU()))
	if err != nil {
		return fmt.Errorf("error creating parquet writer: %v", err)
	}
	defer pw.WriteStop()

	// Set compression for performance
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// Write rows in batches
	batchSize := 1000
	for i := 0; i < df.Rows; i += batchSize {
		end := i + batchSize
		if end > df.Rows {
			end = df.Rows
		}
		batch := make([]interface{}, end-i)
		for j := i; j < end; j++ {
			// Create a struct instance for each row
			rowStruct := reflect.New(schemaType).Elem()
			for colIdx, col := range df.Columns {
				field := rowStruct.FieldByName(strings.Title(col))
				switch df.ColTypes[colIdx] {
				case TypeString:
					sIdx := 0
					for k := 0; k < colIdx; k++ {
						if df.ColTypes[k] == TypeString {
							sIdx++
						}
					}
					if df.StringCols == 0 || sIdx >= len(df.StringData) || j >= len(df.StringData[sIdx]) {
						fmt.Printf("Warning: String data missing at row %d, col %d (sIdx=%d)\n", j, colIdx, sIdx)
						field.SetString("")
					} else {
						field.SetString(df.StringData[sIdx][j])
					}
				case TypeTime:
					tIdx := 0
					for k := 0; k < colIdx; k++ {
						if df.ColTypes[k] == TypeTime {
							tIdx++
						}
					}
					if df.TimeCols == 0 || tIdx >= len(df.TimeData) || j >= len(df.TimeData[tIdx]) {
						fmt.Printf("Warning: Time data missing at row %d, col %d (tIdx=%d)\n", j, colIdx, tIdx)
						field.Set(reflect.Zero(field.Type()))
					} else {
						field.Set(reflect.ValueOf(df.TimeData[tIdx][j]))
					}
				case TypeInt, TypeFloat:
					nIdx := 0
					for k := 0; k < colIdx; k++ {
						if df.ColTypes[k] != TypeString && df.ColTypes[k] != TypeTime {
							nIdx++
						}
					}
					if df.NumericCols == 0 || df.NumericData == nil || nIdx >= df.NumericCols || j >= df.Rows {
						fmt.Printf("Warning: Numeric data missing at row %d, col %d (nIdx=%d)\n", j, colIdx, nIdx)
						field.Set(reflect.Zero(field.Type()))
					} else {
						val := df.NumericData.At(j, nIdx)
						if math.IsNaN(val) {
							field.Set(reflect.Zero(field.Type()))
						} else if df.ColTypes[colIdx] == TypeInt {
							field.SetInt(int64(val))
						} else {
							field.SetFloat(val)
						}
					}
				}
			}
			batch[j-i] = rowStruct.Interface()
		}
		if err := pw.Write(batch); err != nil {
			return fmt.Errorf("error writing batch starting at row %d: %v", i, err)
		}
	}

	return nil
}

// Helper functions
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxFloat(slice []float64) float64 {
	max := slice[0]
	for _, v := range slice[1:] {
		if v > max && !math.IsNaN(v) {
			max = v
		}
	}
	return max
}
