package dataframe

import (
	"fmt"
	"strings"
)

// Join combines two frames on equality of a left and right key column. joinType
// is "inner", "left", "right", "outer"/"full", or "cross". The join key from
// the right frame is dropped from the output (it duplicates the left key);
// other right columns that collide with left names are suffixed "_right".
//
// Both frames must belong to the same engine. Because every DataFrame is just a
// relation in the shared DuckDB instance, the join is a single SQL statement
// rather than the original's manual hash-join over parallel arrays.
//
//	left.Join(right, "inner", "user_id", "id")
func (df *DataFrame) Join(other *DataFrame, joinType, leftOn, rightOn string) (*DataFrame, error) {
	if df.eng != other.eng {
		return nil, fmt.Errorf("Join: frames belong to different engines")
	}
	ls, err := df.loadSchema()
	if err != nil {
		return nil, err
	}
	rs, err := other.loadSchema()
	if err != nil {
		return nil, err
	}

	kind, ok := normalizeJoin(joinType)
	if !ok {
		return nil, fmt.Errorf("Join: unsupported join type %q", joinType)
	}
	if kind != "CROSS" {
		if !ls.Has(leftOn) {
			return nil, fmt.Errorf("Join: left key %q not found", leftOn)
		}
		if !rs.Has(rightOn) {
			return nil, fmt.Errorf("Join: right key %q not found", rightOn)
		}
	}

	// Build the projection: all left columns, then right columns except the
	// join key, with collisions renamed.
	leftNames := make(map[string]struct{}, len(ls.Columns))
	projs := make([]string, 0, len(ls.Columns)+len(rs.Columns))
	for _, c := range ls.Columns {
		leftNames[c] = struct{}{}
		projs = append(projs, "l."+quoteIdent(c)+" AS "+quoteIdent(c))
	}
	for _, c := range rs.Columns {
		if kind != "CROSS" && c == rightOn {
			continue // drop duplicate key
		}
		out := c
		if _, clash := leftNames[c]; clash {
			out = c + "_right"
		}
		projs = append(projs, "r."+quoteIdent(c)+" AS "+quoteIdent(out))
	}

	var onClause string
	if kind == "CROSS" {
		onClause = ""
	} else {
		onClause = fmt.Sprintf(" ON l.%s = r.%s",
			quoteIdent(leftOn), quoteIdent(rightOn))
	}

	// === FIXED: Smart wrapping to avoid invalid ("table_name") syntax ===
	var leftFrom, rightFrom string
	if strings.HasPrefix(df.relation, "(") || strings.HasPrefix(strings.ToUpper(df.relation), "SELECT") {
		leftFrom = "(" + df.relation + ")"
	} else {
		leftFrom = df.relation
	}
	if strings.HasPrefix(other.relation, "(") || strings.HasPrefix(strings.ToUpper(other.relation), "SELECT") {
		rightFrom = "(" + other.relation + ")"
	} else {
		rightFrom = other.relation
	}

	q := fmt.Sprintf("SELECT %s FROM %s AS l %s JOIN %s AS r%s",
		strings.Join(projs, ", "),
		leftFrom,
		kind,
		rightFrom,
		onClause,
	)
	return newDataFrame(df.eng, q), nil
}

func normalizeJoin(t string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "inner", "":
		return "INNER", true
	case "left", "left outer":
		return "LEFT", true
	case "right", "right outer":
		return "RIGHT", true
	case "outer", "full", "full outer":
		return "FULL", true
	case "cross":
		return "CROSS", true
	default:
		return "", false
	}
}

// Concat stacks another frame's rows beneath this one. By default it aligns
// columns by name and unions the two schemas: columns missing from one side are
// filled with NULL on that side (UNION ALL BY NAME). This is more forgiving
// than the original, which required identical layouts.
//
// Both frames must share an engine.
func (df *DataFrame) Concat(other *DataFrame) (*DataFrame, error) {
	if df.eng != other.eng {
		return nil, fmt.Errorf("Concat: frames belong to different engines")
	}
	// UNION ALL BY NAME matches columns by name and fills the rest with NULL,
	// so differing-but-overlapping schemas concatenate sensibly.
	q := fmt.Sprintf("SELECT * FROM (%s) AS _a UNION ALL BY NAME SELECT * FROM (%s) AS _b",
		df.relation, other.relation)
	return newDataFrame(df.eng, q), nil
}
