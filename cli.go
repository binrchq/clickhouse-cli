package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// Terminal 终端接口，用于输入输出
type Terminal interface {
	io.Reader
	io.Writer
}

// CLI ClickHouse 交互式命令行客户端
type CLI struct {
	term          Terminal
	host          string
	port          int
	username      string
	password      string
	database      string
	db            *sql.DB
	reader        *Reader
	serverInfo    ServerInfo
	timingEnabled bool
	verticalMode  bool
	maxRows       int
}

// ServerInfo ClickHouse 服务器信息
type ServerInfo struct {
	Version    string
	Uptime     int64
	BuildType  string
}

// Config ClickHouse 连接配置
type Config struct {
	Host            string
	Port            int
	Username        string
	Password        string
	Database        string
	Secure          bool          // 使用 TLS
	SkipVerify      bool          // 跳过 TLS 验证
	DialTimeout     time.Duration // 连接超时
	ReadTimeout     time.Duration // 读超时
	WriteTimeout    time.Duration // 写超时
	MaxOpenConns    int           // 最大打开连接数
	MaxIdleConns    int           // 最大空闲连接数
	ConnMaxLifetime time.Duration // 连接最大生命周期
	Compression     string        // 压缩方式: lz4, zstd, none
	// 其他参数
	Params map[string]string
}

// NewCLI 创建新的 ClickHouse CLI 实例
func NewCLI(term Terminal, host string, port int, username, password, database string) *CLI {
	return &CLI{
		term:     term,
		host:     host,
		port:     port,
		username: username,
		password: password,
		database: database,
		reader:   NewReader(term),
		maxRows:  1000,
	}
}

// NewCLIWithConfig 使用配置创建 ClickHouse CLI 实例
func NewCLIWithConfig(term Terminal, config *Config) *CLI {
	return &CLI{
		term:     term,
		host:     config.Host,
		port:     config.Port,
		username: config.Username,
		password: config.Password,
		database: config.Database,
		reader:   NewReader(term),
		maxRows:  1000,
	}
}

// Connect 连接到 ClickHouse
func (c *CLI) Connect() error {
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s?dial_timeout=10s&read_timeout=30s",
		c.username, c.password, c.host, c.port, c.database)

	var err error
	c.db, err = sql.Open("clickhouse", dsn)
	if err != nil {
		return err
	}

	c.db.SetMaxOpenConns(10)
	c.db.SetMaxIdleConns(5)
	c.db.SetConnMaxLifetime(time.Hour)

	if err := c.db.Ping(); err != nil {
		c.db.Close()
		return err
	}

	c.fetchServerInfo()
	c.showWelcome()

	return nil
}

// fetchServerInfo 获取服务器信息
func (c *CLI) fetchServerInfo() {
	c.db.QueryRow("SELECT version()").Scan(&c.serverInfo.Version)
	c.db.QueryRow("SELECT uptime()").Scan(&c.serverInfo.Uptime)
}

// showWelcome 显示欢迎信息
func (c *CLI) showWelcome() {
	fmt.Fprintf(c.term, "ClickHouse client version %s\n", c.serverInfo.Version)
	fmt.Fprintf(c.term, "Connecting to %s:%d\n", c.host, c.port)
	fmt.Fprintf(c.term, "Connected to ClickHouse server version %s\n", c.serverInfo.Version)
	fmt.Fprintf(c.term, "\n")
}

// Start 启动交互式命令行
func (c *CLI) Start() error {
	for {
		// 设置提示符
		prompt := c.getPrompt()
		c.reader.SetPrompt(prompt)

		sqlStr := c.readMultiLine()
		if sqlStr == "" {
			continue
		}

		sqlStr = strings.TrimSpace(sqlStr)

		if c.handleSpecialCommand(sqlStr) {
			if strings.ToLower(sqlStr) == "exit" || strings.ToLower(sqlStr) == "quit" {
				return nil
			}
			continue
		}

		c.executeSQL(sqlStr)
	}
}

// getPrompt 获取提示符
func (c *CLI) getPrompt() string {
	if c.database != "" {
		return fmt.Sprintf("%s :) ", c.database)
	}
	return "clickhouse :) "
}

// readMultiLine 读取多行 SQL
func (c *CLI) readMultiLine() string {
	var lines []string

	for {
		line, err := c.reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				return ""
			}
			return ""
		}

		trimmed := strings.TrimSpace(line)
		if trimmed == "" && len(lines) == 0 {
			return ""
		}

		// 如果是第一行，检查是否是特殊命令（不需要分隔符）
		if len(lines) == 0 {
			cmdLower := strings.ToLower(trimmed)
			if cmdLower == "exit" || cmdLower == "quit" || cmdLower == "\\q" || 
			   cmdLower == "help" || cmdLower == "\\h" || 
			   cmdLower == "timing" || cmdLower == "\\timing" {
				return trimmed
			}
		}

		lines = append(lines, line)

		if strings.HasSuffix(trimmed, ";") {
			break
		}

		// 设置多行提示符
		c.reader.SetPrompt(":-] ")
	}

	result := strings.Join(lines, "\n")
	result = strings.TrimSuffix(strings.TrimSpace(result), ";")
	return result
}

// handleSpecialCommand 处理特殊命令
func (c *CLI) handleSpecialCommand(cmd string) bool {
	cmdLower := strings.ToLower(strings.TrimSpace(cmd))

	if cmdLower == "exit" || cmdLower == "quit" || cmdLower == "\\q" {
		fmt.Fprintf(c.term, "Bye\n")
		return true
	}

	if cmdLower == "help" || cmdLower == "\\h" {
		c.showHelp()
		return true
	}

	if cmdLower == "timing" || cmdLower == "\\timing" {
		c.timingEnabled = !c.timingEnabled
		if c.timingEnabled {
			fmt.Fprintf(c.term, "Timing is on.\n")
		} else {
			fmt.Fprintf(c.term, "Timing is off.\n")
		}
		return true
	}

	if cmdLower == "vertical" || cmdLower == "\\G" {
		c.verticalMode = !c.verticalMode
		if c.verticalMode {
			fmt.Fprintf(c.term, "Vertical output mode enabled.\n")
		} else {
			fmt.Fprintf(c.term, "Vertical output mode disabled.\n")
		}
		return true
	}

	if cmdLower == "clear" || cmdLower == "cls" {
		fmt.Fprintf(c.term, "\033[2J\033[H")
		return true
	}

	// ClickHouse specific commands
	if strings.HasPrefix(cmdLower, "use ") {
		parts := strings.Fields(cmd)
		if len(parts) >= 2 {
			c.useDatabase(parts[1])
		}
		return true
	}

	return false
}

// executeSQL 执行 SQL 语句
func (c *CLI) executeSQL(sqlStr string) {
	startTime := time.Now()

	sqlStr = strings.TrimSpace(sqlStr)
	if sqlStr == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if isQuery(sqlStr) {
		c.executeQuery(ctx, sqlStr, startTime)
	} else {
		c.executeCommand(ctx, sqlStr, startTime)
	}
}

// executeQuery 执行查询语句
func (c *CLI) executeQuery(ctx context.Context, sqlStr string, startTime time.Time) {
	rows, err := c.db.QueryContext(ctx, sqlStr)
	if err != nil {
		c.printError(err)
		return
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	colTypes, _ := rows.ColumnTypes()

	if c.verticalMode {
		c.displayVertical(rows, cols, startTime)
	} else {
		c.displayTable(rows, cols, colTypes, startTime)
	}
}

// displayTable 以表格形式显示结果
func (c *CLI) displayTable(rows *sql.Rows, cols []string, colTypes []*sql.ColumnType, startTime time.Time) {
	colWidths := make([]int, len(cols))
	for i, col := range cols {
		colWidths[i] = len(col)
		if colWidths[i] < 4 {
			colWidths[i] = 4
		}
		if colWidths[i] > 50 {
			colWidths[i] = 50
		}
	}

	var allRows [][]string
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		rows.Scan(valPtrs...)

		rowStrs := make([]string, len(vals))
		for i, v := range vals {
			if v == nil {
				rowStrs[i] = ""
			} else {
				switch val := v.(type) {
				case []byte:
					rowStrs[i] = string(val)
				case time.Time:
					rowStrs[i] = val.Format("2006-01-02 15:04:05")
				default:
					rowStrs[i] = fmt.Sprintf("%v", v)
				}
			}

			if len(rowStrs[i]) > colWidths[i] {
				if len(rowStrs[i]) > 50 {
					colWidths[i] = 50
					rowStrs[i] = rowStrs[i][:47] + "..."
				} else {
					colWidths[i] = len(rowStrs[i])
				}
			}
		}
		allRows = append(allRows, rowStrs)

		if len(allRows) >= c.maxRows {
			break
		}
	}

	// ClickHouse style table output
	for i, col := range cols {
		if i > 0 {
			fmt.Fprintf(c.term, " │ ")
		}
		fmt.Fprintf(c.term, "%-*s", colWidths[i], col)
	}
	fmt.Fprintf(c.term, "\n")

	for i := range cols {
		if i > 0 {
			fmt.Fprintf(c.term, "─┼─")
		}
		fmt.Fprintf(c.term, "%s", strings.Repeat("─", colWidths[i]))
	}
	fmt.Fprintf(c.term, "\n")

	for _, row := range allRows {
		for i, val := range row {
			if i > 0 {
				fmt.Fprintf(c.term, " │ ")
			}
			fmt.Fprintf(c.term, "%-*s", colWidths[i], val)
		}
		fmt.Fprintf(c.term, "\n")
	}

	rowCount := len(allRows)
	elapsed := time.Since(startTime).Seconds()

	fmt.Fprintf(c.term, "\n%d rows in set.", rowCount)
	if c.timingEnabled {
		fmt.Fprintf(c.term, " Elapsed: %.3f sec.", elapsed)
	}
	fmt.Fprintf(c.term, "\n\n")
}

// displayVertical 以垂直形式显示结果
func (c *CLI) displayVertical(rows *sql.Rows, cols []string, startTime time.Time) {
	rowNum := 0
	for rows.Next() {
		rowNum++
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		rows.Scan(valPtrs...)

		fmt.Fprintf(c.term, "Row %d:\n", rowNum)
		fmt.Fprintf(c.term, "%s\n", strings.Repeat("─", 50))

		maxColLen := 0
		for _, col := range cols {
			if len(col) > maxColLen {
				maxColLen = len(col)
			}
		}

		for i, col := range cols {
			var valStr string
			if vals[i] == nil {
				valStr = ""
			} else {
				switch val := vals[i].(type) {
				case []byte:
					valStr = string(val)
				case time.Time:
					valStr = val.Format("2006-01-02 15:04:05")
				default:
					valStr = fmt.Sprintf("%v", val)
				}
			}
			fmt.Fprintf(c.term, "%-*s: %s\n", maxColLen, col, valStr)
		}
		fmt.Fprintf(c.term, "\n")

		if rowNum >= c.maxRows {
			break
		}
	}

	elapsed := time.Since(startTime).Seconds()
	fmt.Fprintf(c.term, "%d rows in set.", rowNum)
	if c.timingEnabled {
		fmt.Fprintf(c.term, " Elapsed: %.3f sec.", elapsed)
	}
	fmt.Fprintf(c.term, "\n\n")
}

// executeCommand 执行非查询语句
func (c *CLI) executeCommand(ctx context.Context, sqlStr string, startTime time.Time) {
	result, err := c.db.ExecContext(ctx, sqlStr)
	if err != nil {
		c.printError(err)
		return
	}

	affected, _ := result.RowsAffected()
	elapsed := time.Since(startTime).Seconds()

	fmt.Fprintf(c.term, "Ok. %d rows affected.", affected)
	if c.timingEnabled {
		fmt.Fprintf(c.term, " Elapsed: %.3f sec.", elapsed)
	}
	fmt.Fprintf(c.term, "\n\n")
}

// useDatabase 切换数据库
func (c *CLI) useDatabase(dbName string) {
	c.database = dbName
	fmt.Fprintf(c.term, "Ok.\n")
}

// printError 打印错误信息
func (c *CLI) printError(err error) {
	fmt.Fprintf(c.term, "Code: 0. DB::Exception: %s\n\n", err.Error())
}

// showHelp 显示帮助信息
func (c *CLI) showHelp() {
	help := `
ClickHouse Commands
===================

General:
  help, \\h                Show this help
  exit, quit, \\q         Exit
  clear, cls              Clear screen
  timing, \\timing        Toggle timing
  vertical, \\G           Toggle vertical output

Database:
  USE <database>          Change database
  SHOW DATABASES          List databases
  SHOW TABLES             List tables
  SHOW CREATE TABLE t     Show table DDL

Query Commands:
  SELECT ...              Query data
  SELECT ... FORMAT JSON  Query with JSON format
  SELECT ... FORMAT CSV   Query with CSV format
  INSERT INTO ...         Insert data
  
DDL Commands:
  CREATE TABLE ...        Create table
  CREATE DATABASE ...     Create database
  DROP TABLE ...          Drop table
  ALTER TABLE ...         Alter table
  OPTIMIZE TABLE ...      Optimize table
  
System Tables:
  SELECT * FROM system.databases
  SELECT * FROM system.tables
  SELECT * FROM system.columns WHERE database='db' AND table='t'
  SELECT * FROM system.processes      -- Show running queries
  SELECT * FROM system.query_log      -- Query log
  
ClickHouse Specific:
  DESCRIBE TABLE t        Describe table structure
  EXISTS TABLE t          Check if table exists
  TRUNCATE TABLE t        Truncate table
  RENAME TABLE old TO new Rename table

For more: https://clickhouse.com/docs/
`
	fmt.Fprintf(c.term, help)
}

// Close 关闭数据库连接
func (c *CLI) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// isQuery 判断是否是查询语句
func isQuery(sqlStr string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))

	queryPrefixes := []string{
		"SELECT", "SHOW", "DESC", "DESCRIBE",
		"EXISTS", "EXPLAIN", "WITH",
	}

	for _, prefix := range queryPrefixes {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}

	return false
}

// ParseInt 安全地解析整数
func parseInt(s string) int {
	i, _ := strconv.Atoi(s)
	return i
}

