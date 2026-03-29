# go-pglite

Very hacky embedded PostgreSQL 17.x for Go, powered by [PGlite](https://pglite.dev/) compiled to WASI.

Based on [pglite-oxide](https://github.com/f0rr0/pglite-oxide)

## Usage

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    pglite "github.com/bobTheBuilder7/go-pglite"
)

func main() {
    pg, err := pglite.New(pglite.Config{})
    if err != nil {
        log.Fatal(err)
    }
    defer pg.Close()

    db := pg.DB()

    var greeting string
    err = db.QueryRow("SELECT 'hello from pglite'").Scan(&greeting)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(greeting)
}
```

## Configuration

```go
pglite.Config{
    DataDir:    "/path/to/data",  // empty = temporary directory (removed on close)
    Database:   "mydb",           // default: "postgres"
    User:       "myuser",         // default: "postgres"
    StdoutFile: "/tmp/pg.log",    // empty = /dev/null
    StderrFile: "/tmp/pg.err",    // empty = /dev/null
}
```

## How it works

The PGlite WASI binary (PostgreSQL 17 compiled to WebAssembly) is embedded in the Go binary via `//go:embed`. At runtime, it is extracted and executed using [wasmtime-go](https://github.com/bytecodealliance/wasmtime-go). A Unix socket bridge translates between standard PostgreSQL wire protocol and PGlite's file-based I/O, so any Go SQL driver (this package uses [lib/pq](https://github.com/lib/pq)) can connect to it.

## Requirements

- Go 1.24+
- macOS or Linux (Unix sockets required)

## Running tests

```
cd go-pglite
go test -v -timeout 300s
```

## REPL

```
go run ./cmd/pglite-repl
```
