package pglite_test

import (
	"database/sql"
	"testing"
	"time"

	pglite "github.com/bobTheBuilder7/go-pglite"
)

func newTestPG(t *testing.T) *pglite.PGlite {
	t.Helper()
	pg, err := pglite.New(pglite.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("pglite.New: %v", err)
	}
	t.Cleanup(func() { pg.Close() })
	return pg
}

func TestConnect(t *testing.T) {
	pg := newTestPG(t)
	db := pg.DB()

	var num int
	if err := db.QueryRow("SELECT 1").Scan(&num); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
	if num != 1 {
		t.Errorf("expected 1, got %d", num)
	}
}

func TestParameterizedQuery(t *testing.T) {
	pg := newTestPG(t)
	db := pg.DB()

	var result int
	err := db.QueryRow("SELECT 1 + $1::int", 41).Scan(&result)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if result != 42 {
		t.Errorf("expected 42, got %d", result)
	}
}

func TestMultiQuery(t *testing.T) {
	pg := newTestPG(t)
	db := pg.DB()

	mustExec(t, db, "CREATE TABLE test (id int, name text)")
	mustExec(t, db, "INSERT INTO test VALUES (1, 'alice')")
	mustExec(t, db, "INSERT INTO test VALUES (2, 'bob')")

	rows, err := db.Query("SELECT id, name FROM test ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	type user struct {
		id   int
		name string
	}
	var got []user
	for rows.Next() {
		var u user
		if err := rows.Scan(&u.id, &u.name); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, u)
	}

	want := []user{{1, "alice"}, {2, "bob"}}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestInformationSchema(t *testing.T) {
	pg := newTestPG(t)
	db := pg.DB()

	mustExec(t, db, `CREATE TABLE users (
		id serial PRIMARY KEY,
		name text NOT NULL,
		email varchar(255) UNIQUE,
		age integer DEFAULT 0,
		active boolean DEFAULT true,
		created_at timestamp DEFAULT now()
	)`)

	rows, err := db.Query(`
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = 'users'
		ORDER BY ordinal_position`)
	if err != nil {
		t.Fatalf("information_schema: %v", err)
	}
	defer rows.Close()

	type col struct {
		name, dataType, nullable string
	}
	var cols []col
	for rows.Next() {
		var c col
		rows.Scan(&c.name, &c.dataType, &c.nullable)
		cols = append(cols, c)
	}

	if len(cols) != 6 {
		t.Fatalf("expected 6 columns, got %d: %+v", len(cols), cols)
	}

	expect := map[string]string{
		"id": "integer", "name": "text", "email": "character varying",
		"age": "integer", "active": "boolean", "created_at": "timestamp without time zone",
	}
	for _, c := range cols {
		if want, ok := expect[c.name]; ok && c.dataType != want {
			t.Errorf("column %s: got type %q, want %q", c.name, c.dataType, want)
		}
	}

	for _, c := range cols {
		if c.name == "name" && c.nullable != "NO" {
			t.Errorf("column 'name' should be NOT NULL")
		}
	}
}

func TestPgCatalog(t *testing.T) {
	pg := newTestPG(t)
	db := pg.DB()

	mustExec(t, db, `CREATE TABLE products (id serial PRIMARY KEY, name text, price numeric)`)
	mustExec(t, db, `CREATE INDEX idx_products_name ON products (name)`)

	var oid int
	err := db.QueryRow(`SELECT oid FROM pg_class WHERE relname = 'products' AND relkind = 'r'`).Scan(&oid)
	if err != nil {
		t.Fatalf("pg_class lookup: %v", err)
	}

	rows, err := db.Query(`SELECT indexname FROM pg_indexes WHERE tablename = 'products' ORDER BY indexname`)
	if err != nil {
		t.Fatalf("pg_indexes: %v", err)
	}
	defer rows.Close()

	var indexes []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		indexes = append(indexes, name)
	}

	if len(indexes) < 2 {
		t.Fatalf("expected >= 2 indexes, got %v", indexes)
	}

	found := false
	for _, idx := range indexes {
		if idx == "idx_products_name" {
			found = true
		}
	}
	if !found {
		t.Errorf("idx_products_name not found in %v", indexes)
	}
}

func TestJSONB(t *testing.T) {
	pg := newTestPG(t)
	db := pg.DB()

	mustExec(t, db, `CREATE TABLE docs (id serial PRIMARY KEY, data jsonb)`)
	_, err := db.Exec(`INSERT INTO docs (data) VALUES ($1)`, `{"name": "alice", "tags": ["admin", "user"]}`)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var name string
	err = db.QueryRow(`SELECT data->>'name' FROM docs WHERE data @> '{"tags": ["admin"]}'`).Scan(&name)
	if err != nil {
		t.Fatalf("jsonb query: %v", err)
	}
	if name != "alice" {
		t.Errorf("expected 'alice', got %q", name)
	}
}

func TestCascadeDelete(t *testing.T) {
	pg := newTestPG(t)
	db := pg.DB()

	mustExec(t, db, `CREATE TABLE departments (id serial PRIMARY KEY, name text NOT NULL)`)
	mustExec(t, db, `CREATE TABLE employees (id serial PRIMARY KEY, name text, dept_id int REFERENCES departments(id) ON DELETE CASCADE)`)
	mustExec(t, db, `INSERT INTO departments (name) VALUES ('engineering')`)
	mustExec(t, db, `INSERT INTO employees (name, dept_id) VALUES ('alice', 1)`)

	// CASCADE delete.
	mustExec(t, db, `DELETE FROM departments WHERE id = 1`)
	var count int
	db.QueryRow(`SELECT count(*) FROM employees`).Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 employees after CASCADE, got %d", count)
	}
}

func TestVersion(t *testing.T) {
	pg := newTestPG(t)
	db := pg.DB()

	var version string
	db.QueryRow("SELECT version()").Scan(&version)
	t.Logf("PostgreSQL version: %s", version)
	if version == "" {
		t.Error("version string is empty")
	}
}

func TestValueTypes(t *testing.T) {
	pg := newTestPG(t)
	db := pg.DB()

	t.Run("int", func(t *testing.T) {
		var v int
		db.QueryRow("SELECT 42").Scan(&v)
		if v != 42 {
			t.Errorf("expected 42, got %d", v)
		}
	})

	t.Run("text", func(t *testing.T) {
		var v string
		db.QueryRow("SELECT 'hello world'").Scan(&v)
		if v != "hello world" {
			t.Errorf("expected 'hello world', got %q", v)
		}
	})

	t.Run("bool", func(t *testing.T) {
		var v bool
		db.QueryRow("SELECT true").Scan(&v)
		if !v {
			t.Errorf("expected true")
		}
	})

	t.Run("float", func(t *testing.T) {
		var v float64
		db.QueryRow("SELECT 3.14::float8").Scan(&v)
		if v != 3.14 {
			t.Errorf("expected 3.14, got %f", v)
		}
	})

	t.Run("timestamp", func(t *testing.T) {
		var v time.Time
		db.QueryRow("SELECT '2024-06-15 12:00:00+00'::timestamptz").Scan(&v)
		want := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
		if !v.Equal(want) {
			t.Errorf("expected %v, got %v", want, v)
		}
	})
}

func mustExec(t testing.TB, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}
