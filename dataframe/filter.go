package dataframe

import (
	"fmt"
	"strings"
)

// Filtering in the original package took either a "col op val" string or a Go
// func(int) bool. A Go predicate can't be pushed into SQL, so the row-callback
// form is gone; in its place we offer three composable options that all run
// inside DuckDB:
//
//   - Filter(cond string): the familiar "col op value" shorthand, extended with
//     more operators and safe value handling.
//   - Where(sqlExpr string): a raw SQL boolean expression for anything complex.
//   - FilterBy(Predicate): a small structured builder for programmatic filters.
//
// All three are lazy and return a new DataFrame.

// Where filters rows by a raw SQL boolean expression evaluated per row, e.g.
//
//	df.Where("age > 30 AND city = 'Paris'")
//	df.Where("score IS NULL OR score < 0")
//
// The expression is trusted caller SQL. This is the most flexible filter and
// the right tool when the shorthand isn't expressive enough.
func (df *DataFrame) Where(sqlExpr string) *DataFrame {
	q := fmt.Sprintf("SELECT * FROM %s WHERE %s", srcToken, sqlExpr)
	return df.derive(q)
}

// Filter applies a simple "column operator value" condition, parsed from a
// string for convenience and backward familiarity. Supported operators:
// =, ==, !=, <>, <, <=, >, >=, and the word forms LIKE / NOT LIKE.
//
//	df.Filter("age > 30")
//	df.Filter("name = Alice")
//	df.Filter("city LIKE San%")
//
// The value is bound as a typed literal based on the column's type, so it is
// quoted correctly and is injection-safe. For multi-clause logic use Where.
func (df *DataFrame) Filter(condition string) (*DataFrame, error) {
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}

	// Split into exactly three logical parts: column, operator, value. The
	// value may itself contain spaces, so we don't use a naive 3-way split.
	col, op, val, perr := parseCondition(condition)
	if perr != nil {
		return nil, perr
	}
	if !s.Has(col) {
		return nil, fmt.Errorf("Filter: unknown column %q", col)
	}

	sqlOp, ok := normalizeOp(op)
	if !ok {
		return nil, fmt.Errorf("Filter: unsupported operator %q", op)
	}

	t, _ := s.TypeOf(col)
	coerced := coerceToType(strings.TrimSpace(val), t)
	if coerced == nil {
		return nil, fmt.Errorf("Filter: value %q is not valid for %s column %q", val, t, col)
	}
	lit := quoteLit(coerced)
	expr := fmt.Sprintf("%s %s %s", quoteIdent(col), sqlOp, lit)
	q := fmt.Sprintf("SELECT * FROM %s WHERE %s", srcToken, expr)
	return df.derive(q), nil
}

// parseCondition splits "col op value" into its three parts. The column is the
// first whitespace-delimited token and the operator is the second; everything
// after the operator token is the value (which may itself contain spaces, as in
// a LIKE pattern or a quoted string). Splitting on the first two tokens avoids
// the ambiguity of searching for the operator substring inside the whole
// condition.
func parseCondition(cond string) (col, op, val string, err error) {
	// SplitN with N=3 on runs of whitespace would mishandle multiple spaces, so
	// pull the first two tokens via Fields and recover the value by length.
	fields := strings.Fields(cond)
	if len(fields) < 3 {
		return "", "", "", fmt.Errorf("Filter: condition %q must be 'column operator value'", cond)
	}
	col = fields[0]
	op = fields[1]

	// Find where the value starts: skip past the column token, then past the
	// operator token, consuming the whitespace between and after them.
	rest := strings.TrimLeft(cond, " \t")
	rest = strings.TrimPrefix(rest, col)
	rest = strings.TrimLeft(rest, " \t")
	rest = strings.TrimPrefix(rest, op)
	val = strings.TrimSpace(rest)
	if val == "" {
		return "", "", "", fmt.Errorf("Filter: condition %q is missing a value", cond)
	}
	return col, op, val, nil
}

// normalizeOp maps the accepted shorthand operators onto SQL operators.
func normalizeOp(op string) (string, bool) {
	switch strings.ToUpper(op) {
	case "=", "==":
		return "=", true
	case "!=", "<>":
		return "<>", true
	case "<":
		return "<", true
	case "<=":
		return "<=", true
	case ">":
		return ">", true
	case ">=":
		return ">=", true
	case "LIKE":
		return "LIKE", true
	case "NOT_LIKE", "NOTLIKE":
		return "NOT LIKE", true
	default:
		return "", false
	}
}

// Predicate is a structured, programmatic filter clause. Build one and pass it
// to FilterBy. Values are bound as typed literals, so Predicates are safe to
// construct from untrusted input (unlike Where, which takes raw SQL).
type Predicate struct {
	Column string
	Op     string // one of the operators accepted by normalizeOp
	Value  interface{}
}

// FilterBy applies one or more Predicates joined by AND. Passing several
// predicates is the common case ("age > 30 AND city = Paris") without writing
// SQL by hand.
func (df *DataFrame) FilterBy(preds ...Predicate) (*DataFrame, error) {
	if len(preds) == 0 {
		return df, nil
	}
	s, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	clauses := make([]string, len(preds))
	for i, p := range preds {
		if !s.Has(p.Column) {
			return nil, fmt.Errorf("FilterBy: unknown column %q", p.Column)
		}
		sqlOp, ok := normalizeOp(p.Op)
		if !ok {
			return nil, fmt.Errorf("FilterBy: unsupported operator %q", p.Op)
		}
		t, _ := s.TypeOf(p.Column)
		lit := quoteLit(coerceToType(p.Value, t))
		clauses[i] = fmt.Sprintf("%s %s %s", quoteIdent(p.Column), sqlOp, lit)
	}
	q := fmt.Sprintf("SELECT * FROM %s WHERE %s",
		srcToken, strings.Join(clauses, " AND "))
	return df.derive(q), nil
}
