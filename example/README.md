# ClickHouse CLI 使用示例

## 基本使用

```go
cli := clickhousecli.NewCLI(term, "localhost", 9000, "default", "password", "default")
cli.Connect()
cli.Start()
```

## 特点

- 列式数据库
- 支持垂直/水平显示
- 高性能分析查询

## 常用命令

- `USE <database>` - 切换数据库
- `SHOW DATABASES` - 列出数据库
- `SHOW TABLES` - 列出表
- `DESCRIBE TABLE` - 描述表
- `timing` - 切换计时
- `vertical` - 切换垂直显示

## 系统表

```sql
SELECT * FROM system.databases
SELECT * FROM system.tables
SELECT * FROM system.processes
```

## SQL 命令

- `SELECT` - 查询（支持复杂分析）
- `INSERT` - 插入
- `CREATE TABLE` - 创建表
- `DROP TABLE` - 删除表
- `OPTIMIZE TABLE` - 优化表


