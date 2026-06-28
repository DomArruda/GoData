package dataframe

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	_ "github.com/duckdb/duckdb-go/v2"
)

// engine wraps a single in-process DuckDB instance. All DataFrames created
// against the same engine share its connection pool, which lets them be
// joined, concatenated, and combined without copying data across processes.
//
// DuckDB lives in-process, so the *sql.DB handle is the whole database. We
// keep one default engine for convenience (most callers never need more than
// one), but callers who want isolation can construct their own with NewEngine.
type Engine struct {
	db      *sql.DB
	counter atomic.Uint64 // monotonic source of unique relation names
}

var (
	defaultEngine     *Engine
	defaultEngineOnce sync.Once
	defaultEngineErr  error
)

// getDefaultEngine lazily opens a shared in-memory DuckDB instance. The first
// caller to hit an error caches it; subsequent callers see the same error
// rather than racing to re-open.
func getDefaultEngine() (*Engine, error) {
	defaultEngineOnce.Do(func() {
		defaultEngine, defaultEngineErr = NewEngine("")
	})
	return defaultEngine, defaultEngineErr
}

// NewEngine opens a DuckDB instance at the given DSN. An empty DSN opens a
// fresh in-memory database. A file path opens (or creates) a persistent one.
// DuckDB config options may be appended as query parameters, e.g.
// "/tmp/foo.db?threads=4&access_mode=read_only".
func NewEngine(dsn string) (*Engine, error) {
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening duckdb: %w", err)
	}
	// Temporary relations are connection-scoped in DuckDB. database/sql's pool
	// can hand back a different physical connection per query, which would make
	// connection-local temp objects vanish unpredictably. We avoid temp views
	// entirely (every frame is either a base table or an inline subquery), but
	// we still disable idle-connection caching so persistent databases flush
	// their WAL promptly on Close.
	db.SetMaxIdleConns(0)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connecting to duckdb: %w", err)
	}
	return &Engine{db: db}, nil
}

// Close releases the underlying DuckDB instance. After Close, every DataFrame
// derived from this engine is unusable. Closing the defaultSQL  engine is rarely
// necessary for in-memory use but matters for persistent databases, which only
// synchronize fully to disk on Close.
func (e *Engine) Close() error {
	return e.db.Close()
}

// newName returns a process-unique identifier suitable for a DuckDB relation.
func (e *Engine) newName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, e.counter.Add(1))
}

// DataFrame is a lazy handle to a tabular relation living inside DuckDB.
//
// Unlike a materialized in-memory frame, a DataFrame mostly holds a SQL query.
// Transformations (Select, Filter, GroupBy, Sort, Join, ...) don't move any
// data: they wrap the current relation in a new query and return a new
// DataFrame. Data only crosses back into Go when you read it (Collect, Print,
// WriteToCSV, scalar aggregations, ...).
//
// A DataFrame is safe for concurrent reads. Operations never mutate the
// receiver; they always return a new DataFrame, so there is no shared mutable
// state to guard beyond the engine's own pool.
type DataFrame struct {
	eng *Engine

	// relation is a SQL expression that, when substituted into
	// "SELECT ... FROM ( <relation> )", yields this frame's rows. It is either
	// a base table name or a parenthesized subquery. We never interpolate user
	// data into it; only structural SQL we generate ourselves.
	relation string

	// ownedTable, if non-empty, is the name of a base table this frame created
	// and is responsible for. Frames produced by transformations are subqueries
	// and own nothing (ownedTable == ""); only ingestion (FromMaps, ReadCSV,
	// Materialize, ...) creates owned tables. Release drops the owned table.
	ownedTable string

	// empty marks a frame with no columns and no rows. DuckDB cannot represent a
	// zero-column table, so we model that degenerate case (e.g. FromMaps(nil))
	// with this flag instead of a relation, and the read paths short-circuit.
	empty bool

	// schema is cached column metadata. It is populated on demand (lazily) the
	// first time something needs column names/types, then reused.
	schemaOnce sync.Once
	schemaErr  error
	schema     *Schema
}

// emptyFrame returns a frame with no columns and no rows.
func emptyFrame(e *Engine) *DataFrame {
	return &DataFrame{eng: e, empty: true}
}

// newDataFrame constructs a frame over an arbitrary relation expression with no
// owned storage (used for derived/transformed frames).
func newDataFrame(e *Engine, relation string) *DataFrame {
	return &DataFrame{eng: e, relation: relation}
}

// newOwnedDataFrame constructs a frame backed by a base table it owns. The
// relation is the quoted table name; Release will drop it.
func newOwnedDataFrame(e *Engine, table string) *DataFrame {
	return &DataFrame{eng: e, relation: quoteIdent(table), ownedTable: table}
}

// derive builds a child frame whose rows come from wrapping this frame's
// relation in a new SELECT. The {{src}} token in queryTemplate is replaced with
// a parenthesized reference to the current relation aliased as src.
func (df *DataFrame) derive(queryTemplate string) *DataFrame {
	rel := replaceSrc(queryTemplate, df.relation)
	return newDataFrame(df.eng, rel)
}

// Engine exposes the underlying engine, mainly so related frames can be built
// against the same DuckDB instance.
func (df *DataFrame) Engine() *Engine { return df.eng }

// SQL returns the SQL query that defines this frame. Useful for debugging and
// for handing off to other tools.

func (df *DataFrame) SQL() string {
	if strings.HasPrefix(df.relation, "(") || strings.HasPrefix(strings.ToUpper(df.relation), "SELECT") {
		return "SELECT * FROM (" + df.relation + ") AS _df"
	}
	return "SELECT * FROM " + df.relation + " AS _df"
}

// Materialize executes this frame's query once and stores the result in a new
// base table, returning a frame over that table. This is useful when a frame is
// the product of an expensive query chain that you will read repeatedly: rather
// than re-running the whole chain on each read, you pay for it once here.
//
// The returned frame owns its table; call Release on it when done (especially
// on the long-lived default engine) to free the memory. The original frame is
// unchanged.
func (df *DataFrame) Materialize() (*DataFrame, error) {
	name := df.eng.newName("df")
	q := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM (%s) AS _src",
		quoteIdent(name), df.relation)
	if _, err := df.eng.db.Exec(q); err != nil {
		return nil, fmt.Errorf("materializing frame: %w", err)
	}
	return newOwnedDataFrame(df.eng, name), nil
}

// Release drops the base table backing this frame, if it owns one. Frames built
// by transformations own no storage, so Release is a no-op for them and only
// the ingestion/Materialize roots need releasing. After Release, this frame and
// any frames derived from it are invalid.
//
// Because the package's default engine is a single long-lived in-memory DuckDB
// instance, base tables accumulate until released or until the process exits.
// Short programs can ignore this; long-running services that create many frames
// should Release ingestion roots (or use a dedicated Engine they Close) to keep
// memory bounded.
func (df *DataFrame) Release() error {
	if df.ownedTable == "" {
		return nil
	}
	q := fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(df.ownedTable))
	if _, err := df.eng.db.Exec(q); err != nil {
		return fmt.Errorf("releasing frame: %w", err)
	}
	df.ownedTable = ""
	return nil
}
