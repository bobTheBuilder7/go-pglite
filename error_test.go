package pglite_test

import (
	"testing"

	pglite "github.com/elliots/go-pglite"
)

func TestErrorRecovery(t *testing.T) {
	pg, err := pglite.New(pglite.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()
	db := pg.DB()

	// 1. Syntax error → recover → good query.
	_, err = db.Exec(`SELEKT 1`)
	if err == nil {
		t.Fatal("expected syntax error")
	}
	t.Logf("Error 1 (expected): %v", err)

	var v int
	if err := db.QueryRow("SELECT 42").Scan(&v); err != nil {
		t.Fatalf("query after error 1 failed: %v", err)
	}
	t.Logf("Recovery 1: SELECT 42 = %d", v)

	// 2. Missing table → recover → good query.
	_, err = db.Exec("SELECT * FROM nonexistent_table")
	if err == nil {
		t.Fatal("expected table error")
	}
	t.Logf("Error 2 (expected): %v", err)

	if err := db.QueryRow("SELECT 100").Scan(&v); err != nil {
		t.Fatalf("query after error 2 failed: %v", err)
	}
	t.Logf("Recovery 2: SELECT 100 = %d", v)

	// 3. Another error → recover → good query.
	_, err = db.Exec("DROP TABLE nope")
	if err == nil {
		t.Fatal("expected drop error")
	}
	t.Logf("Error 3 (expected): %v", err)

	var msg string
	if err := db.QueryRow("SELECT 'alive'").Scan(&msg); err != nil {
		t.Fatalf("query after error 3 failed: %v", err)
	}
	t.Logf("Recovery 3: SELECT 'alive' = %s", msg)
}
