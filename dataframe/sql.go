package dataframe

import (
	"fmt"
	"strings"
	"time"
)

// This file centralizes everything related to building SQL text safely.
//
// The single most important rule: identifiers and literals that originate from
// column names or user-supplied values MUST go through quoteIdent / quoteLit.
// Never use fmt.Sprintf("%s") to splice a column name or value straight into a
// query. DuckDB follows standard SQL quoting, so we double the relevant quote
// character to escape it.

// quoteIdent wraps a SQL identifier (table or column name) in double quotes,
// escaping any embedded double quotes. This makes identifiers with spaces,
// reserved words, or punctuation safe to use.

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// quoteIdents quotes and comma-joins a list of identifiers.
func quoteIdents(names []string) string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = quoteIdent(n)
	}
	return strings.Join(out, ", ")
}

// quoteLit renders a Go value as a SQL literal. We prefer bound parameters
// (placeholders) wherever a query is executed directly, but some constructs
// (e.g. building a VALUES clause for an in-line table, or a default in a
// COALESCE) need an inline literal. Keep this in sync with the types we accept.
func quoteLit(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	case time.Time:
		// DuckDB parses ISO-8601; render with explicit TIMESTAMP type.
		return "TIMESTAMP '" + x.UTC().Format("2006-01-02 15:04:05.999999") + "'"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", x)
	case float32, float64:
		return fmt.Sprintf("%v", x)
	default:
		// Fall back to a quoted string rendering rather than risk an injection.
		return "'" + strings.ReplaceAll(fmt.Sprintf("%v", x), "'", "''") + "'"
	}
}

// srcToken is the placeholder used in query templates that derive() replaces
// with the parent relation.
const srcToken = "{{src}}"

func wrapForFrom(rel string) string {
	trim := strings.TrimSpace(rel)
	if strings.HasPrefix(trim, "(") || strings.HasPrefix(strings.ToUpper(trim), "SELECT") {
		return "(" + rel + ")"
	}
	// bare table name (e.g. "df_123") or simple expression — use directly
	return rel
}

// replaceSrc substitutes the parent relation into a query template, aliasing it
// as "src" so templates can refer to "src" unambiguously.
func replaceSrc(template, relation string) string {
	wrapped := wrapForFrom(relation)
	return strings.ReplaceAll(template, srcToken, wrapped+" AS src")
}
