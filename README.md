# ClickHouse CLI

[![Go Reference](https://pkg.go.dev/badge/github.com/binrchq/clickhouse-cli.svg)](https://pkg.go.dev/github.com/binrchq/clickhouse-cli)
[![Go Report Card](https://goreportcard.com/badge/github.com/binrchq/clickhouse-cli)](https://goreportcard.com/report/github.com/binrchq/clickhouse-cli)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A standalone ClickHouse interactive CLI client for Go applications.

## Features

- 🚀 Full ClickHouse SQL support
- 📊 Vertical/Horizontal display modes
- ⏱️ Query timing
- 💾 Connection pooling
- 🎯 System tables support
- 📈 Optimized for analytical queries

## Installation

```bash
go get github.com/binrchq/clickhouse-cli
```

## Quick Start

```go
package main

import (
    "log"
    "os"
    
    clickhousecli "github.com/binrchq/clickhouse-cli"
)

func main() {
    cli := clickhousecli.NewCLI(
        os.Stdin,
        "localhost",
        9000,
        "default",
        "password",
        "default",
    )
    
    if err := cli.Connect(); err != nil {
        log.Fatal(err)
    }
    defer cli.Close()
    
    if err := cli.Start(); err != nil {
        log.Fatal(err)
    }
}
```

## Supported Commands

### SQL Commands
- `SELECT` - Query (with complex analytics support)
- `INSERT` - Insert data
- `CREATE TABLE` - Create table
- `DROP TABLE` - Delete table
- `OPTIMIZE TABLE` - Optimize table

### System Tables
```sql
SELECT * FROM system.databases
SELECT * FROM system.tables
SELECT * FROM system.processes
SELECT * FROM system.query_log
```

### Special Commands
- `USE <database>` - Switch database
- `SHOW DATABASES` - List databases
- `SHOW TABLES` - List tables
- `DESCRIBE TABLE` - Describe table
- `help` - Show help
- `timing` - Toggle timing
- `vertical` - Toggle vertical output

## Requirements

- Go 1.21 or higher
- ClickHouse 20.3 or higher

## Dependencies

- [github.com/ClickHouse/clickhouse-go/v2](https://github.com/ClickHouse/clickhouse-go) - ClickHouse driver
- [github.com/chzyer/readline](https://github.com/chzyer/readline) - Readline library

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Author

Maintained by [binrc](https://github.com/binrchq).

## Related Projects

- [mysql-cli](https://github.com/binrchq/mysql-cli) - MySQL CLI
- [postgres-cli](https://github.com/binrchq/postgres-cli) - PostgreSQL CLI
