package analyze

import (
	"cassette-tape/db"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/jedib0t/go-pretty/list"
	"github.com/jedib0t/go-pretty/table"
	"github.com/pingcap/tidb/pkg/parser"
	"go.uber.org/zap"
)

type report struct {
	db                    *db.DuckDB
	timeRange             string
	totalQueriesCount     string
	queryTypeDistribution queryTypeDistribution
	highFrequencyQueries  []highFrequencyQueries
}

func newReport(duckdb *db.DuckDB) *report {
	r := &report{}
	r.db = duckdb
	r.getTimeRange()
	r.getTotalQueriesCount()
	r.getQueryTypeDistribution()
	r.getHighFrequencyQueries()
	return r
}

func (r *report) render() {
	l := list.NewWriter()
	l.SetStyle(list.StyleConnectedRounded)

	tb := table.NewWriter()
	tb.SetStyle(table.StyleLight)
	tb.SetTitle("📊 Workload Analysis Report")
	tb.AppendRow(
		table.Row{r.timeRange})
	l.AppendItem(tb.Render())

	tb = table.NewWriter()
	tb.SetStyle(table.StyleLight)
	tb.SetTitle("🌧️ Query Type Distribution")
	tb.AppendRow(
		table.Row{
			"TOTAL", "SELECT", "INSERT", "UPDATE", "DELETE", "COMMIT", "ROLLBACK", "DDL", "ANALYZE", "UNKNOWN"})
	tb.AppendRow(
		table.Row{
			r.totalQueriesCount,
			r.queryTypeDistribution.SELECT.Percent,
			r.queryTypeDistribution.INSERT.Percent,
			r.queryTypeDistribution.UPDATE.Percent,
			r.queryTypeDistribution.DELETE.Percent,
			r.queryTypeDistribution.COMMIT.Percent,
			r.queryTypeDistribution.ROLLBACK.Percent,
			r.queryTypeDistribution.DDL.Percent,
			r.queryTypeDistribution.ANALYZE.Percent,
			r.queryTypeDistribution.UNKNOWN.Percent})
	l.AppendItem(tb.Render())
	l.UnIndent()

	tb = table.NewWriter()
	tb.SetStyle(table.StyleLight)
	tb.SetTitle("🔥 High Frequency queries")
	tb.AppendHeader(table.Row{"Query", "Count"})
	for _, row := range r.highFrequencyQueries {
		tb.AppendRow(
			table.Row{row.text, row.count})
	}
	l.AppendItem(tb.Render())
	l.UnIndent()

	fmt.Println(l.Render())
}

func (r *report) getTimeRange() {
	query := fmt.Sprintf(`SELECT CONCAT(
		STRFTIME(MIN(timestamp), '%%Y-%%m-%%d %%H:%%M:%%S'), ' - ' ,STRFTIME(MAX(timestamp), '%%Y-%%m-%%d %%H:%%M:%%S')
	) FROM %s`, db.TableName)
	var timeRange string
	err := r.db.Conn.QueryRow(query).Scan(&timeRange)
	if err != nil {
		log.Fatal("failed to get timeRange", zap.Error(err))
	}
	r.timeRange = timeRange
}

func (r *report) getTotalQueriesCount() {
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, db.TableName)
	var count string
	err := r.db.Conn.QueryRow(query).Scan(&count)
	if err != nil {
		log.Fatal("failed to get total-SQL-count", zap.Error(err))
	}
	r.totalQueriesCount = count
}

type queryType struct {
	Count   int64  `json:"count"`
	Percent string `json:"percent"`
}

type queryTypeDistribution struct {
	SELECT   queryType `json:"select"`
	INSERT   queryType `json:"insert"`
	UPDATE   queryType `json:"update"`
	DELETE   queryType `json:"delete"`
	COMMIT   queryType `json:"commit"`
	ROLLBACK queryType `json:"rollback"`
	DDL      queryType `json:"ddl"`
	ANALYZE  queryType `json:"analyze"`
	UNKNOWN  queryType `json:"unknown"`
}

func (r *report) getQueryTypeDistribution() {
	sql := fmt.Sprintf(`WITH type_counts AS (
		SELECT TYPE, COUNT(*) as count FROM %s GROUP BY TYPE
	),
	total_count AS (
		SELECT COUNT(*) as total FROM %s
	)
	SELECT
		COALESCE(SUM(CASE WHEN TYPE = 'select' THEN count END), 0) AS select_count,
		CONCAT(COALESCE(SUM(CASE WHEN TYPE = 'select' THEN count END), 0), ' (', ROUND(COALESCE(SUM(CASE WHEN TYPE = 'select' THEN count END), 0) * 100.0 / ANY_VALUE(total), 3), '%%', ')') AS select_percent,
		COALESCE(SUM(CASE WHEN TYPE = 'insert' THEN count END), 0) AS insert_count,
		CONCAT(COALESCE(SUM(CASE WHEN TYPE = 'insert' THEN count END), 0), ' (', ROUND(COALESCE(SUM(CASE WHEN TYPE = 'insert' THEN count END), 0) * 100.0 / ANY_VALUE(total), 3), '%%', ')') AS insert_percent,
		COALESCE(SUM(CASE WHEN TYPE = 'update' THEN count END), 0) AS update_count,
		CONCAT(COALESCE(SUM(CASE WHEN TYPE = 'update' THEN count END), 0), ' (', ROUND(COALESCE(SUM(CASE WHEN TYPE = 'update' THEN count END), 0) * 100.0 / ANY_VALUE(total), 3), '%%', ')') AS update_percent,
		COALESCE(SUM(CASE WHEN TYPE = 'delete' THEN count END), 0) AS delete_count,
		CONCAT(COALESCE(SUM(CASE WHEN TYPE = 'delete' THEN count END), 0), ' (', ROUND(COALESCE(SUM(CASE WHEN TYPE = 'delete' THEN count END), 0) * 100.0 / ANY_VALUE(total), 3), '%%', ')') AS delete_percent,
		COALESCE(SUM(CASE WHEN TYPE = 'commit' THEN count END), 0) AS commit_count,
		CONCAT(COALESCE(SUM(CASE WHEN TYPE = 'commit' THEN count END), 0), ' (', ROUND(COALESCE(SUM(CASE WHEN TYPE = 'commit' THEN count END), 0) * 100.0 / ANY_VALUE(total), 3), '%%', ')') AS commit_percent,
		COALESCE(SUM(CASE WHEN TYPE = 'rollback' THEN count END), 0) AS rollback_count,
		CONCAT(COALESCE(SUM(CASE WHEN TYPE = 'rollback' THEN count END), 0), ' (', ROUND(COALESCE(SUM(CASE WHEN TYPE = 'rollback' THEN count END), 0) * 100.0 / ANY_VALUE(total), 3), '%%', ')') AS rollback_percent,
		COALESCE(SUM(CASE WHEN TYPE = 'ddl' THEN count END), 0) AS ddl_count,
		CONCAT(COALESCE(SUM(CASE WHEN TYPE = 'ddl' THEN count END), 0), ' (', ROUND(COALESCE(SUM(CASE WHEN TYPE = 'ddl' THEN count END), 0) * 100.0 / ANY_VALUE(total), 3), '%%', ')') AS ddl_percent,
		COALESCE(SUM(CASE WHEN TYPE = 'analyze' THEN count END), 0) AS analyze_count,
		CONCAT(COALESCE(SUM(CASE WHEN TYPE = 'analyze' THEN count END), 0), ' (', ROUND(COALESCE(SUM(CASE WHEN TYPE = 'analyze' THEN count END), 0) * 100.0 / ANY_VALUE(total), 3), '%%', ')') AS analyze_percent,
		COALESCE(SUM(CASE WHEN TYPE = 'unknown' THEN count END), 0) AS unknown_count,
		CONCAT(COALESCE(SUM(CASE WHEN TYPE = 'unknown' THEN count END), 0), ' (', ROUND(COALESCE(SUM(CASE WHEN TYPE = 'unknown' THEN count END), 0) * 100.0 / ANY_VALUE(total), 3), '%%', ')') AS unknown_percent
	FROM type_counts, total_count`, db.TableName, db.TableName)

	rs, err := r.db.Conn.Query(sql)
	if err != nil {
		log.Fatal("failed to get SQL-type-distribution", zap.Error(err))
	}
	defer rs.Close()

	var q queryTypeDistribution
	if rs.Next() {
		err := rs.Scan(
			&q.SELECT.Count, &q.SELECT.Percent,
			&q.INSERT.Count, &q.INSERT.Percent,
			&q.UPDATE.Count, &q.UPDATE.Percent,
			&q.DELETE.Count, &q.DELETE.Percent,
			&q.COMMIT.Count, &q.COMMIT.Percent,
			&q.ROLLBACK.Count, &q.ROLLBACK.Percent,
			&q.DDL.Count, &q.DDL.Percent,
			&q.ANALYZE.Count, &q.ANALYZE.Percent,
			&q.UNKNOWN.Count, &q.UNKNOWN.Percent,
		)
		if err != nil {
			log.Fatal("failed to get SQL-type-distribution", zap.Error(err))
		}
	}
	r.queryTypeDistribution = q
}

type highFrequencyQueries struct {
	text  string
	count int
}

func (r *report) getHighFrequencyQueries() {

	query := fmt.Sprintf(`SELECT FIRST(text), COUNT(*) AS count FROM %s GROUP BY digest ORDER BY COUNT(*) DESC LIMIT 20`, db.TableName)

	rs, err := r.db.Conn.Query(query)
	if err != nil {
		log.Fatal("failed to set high-frequency-queries", zap.Error(err))
	} else {
		defer func(rs *sql.Rows) {
			_ = rs.Close()
		}(rs)
		hs := make([]highFrequencyQueries, 0)
		h := highFrequencyQueries{}
		for rs.Next() {
			if err := rs.Scan(&h.text, &h.count); err != nil {
				log.Fatal("failed to set high-frequency-queries", zap.Error(err))
			}
			normalizeText := parser.NormalizeForBinding(h.text, false)
			h.text = verb(normalizeText)
			hs = append(hs, h)
		}
		r.highFrequencyQueries = hs
	}
}

func verb(str string) string {
	keywords := []string{
		"ACCESSIBLE", "ADD", "ALL", "ALTER", "ANALYSE", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE",
		"BEFORE", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH", "BY",
		"CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK", "COMMIT", "COLLATE", "COLUMN", "CONDITION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CUBE", "CURRENT", "CURSOR",
		"DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELAYED", "DELETE", "DESC", "DESCRIBE", "DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL",
		"EACH", "ELSE", "ELSEIF", "EMPTY", "ENCLOSED", "ESCAPED", "EXCEPT", "EXISTS", "EXIT", "EXPLODE", "EXPLAIN",
		"FALSE", "FETCH", "FLOAT", "FLOAT4", "FLOAT8", "FOR", "FORCE", "FOREIGN", "FROM", "FULLTEXT",
		"GENERATED", "GET", "GRANT", "GROUP", "GROUPING",
		"HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND",
		"IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT", "INT1", "INT2", "INT3", "INT4", "INT8", "INTEGER", "INTERVAL", "INTO", "IO_AFTER_GTIDS", "IO_BEFORE_GTIDS", "IS", "ITERATE",
		"JOIN",
		"KEY", "KEYS", "KILL",
		"LANGUAGE", "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT", "LINEAR", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY",
		"MASTER_BIND", "MASTER_SSL_VERIFY_SERVER_CERT", "MATCH", "MAXVALUE", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES",
		"NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NULL", "NUMERIC",
		"OF", "ON", "OPTIMIZE", "OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER", "OUTFILE", "OVER",
		"PARTITION", "PRECISION", "PRIMARY", "PRIVILEGES", "PROCEDURE", "PURGE",
		"RANGE", "READ", "READS", "READ_WRITE", "REAL", "RECURSIVE", "REFERENCES", "REGEXP", "RELEASE", "RENAME", "REPEAT", "REPLACE", "REQUIRE", "RESIGNAL", "RESTRICT", "RETURN", "REVOKE", "RIGHT", "RLIKE", "ROLLBACK",
		"SCHEMA", "SCHEMAS", "SECOND_MICROSECOND", "SELECT", "SENSITIVE", "SEPARATOR", "SET", "SHOW", "SIGNAL", "SMALLINT", "SPATIAL", "SPECIFIC", "sql", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING", "STORED", "STRAIGHT_JOIN", "SYSTEM",
		"TABLE", "TABLES", "TEMPORARY", "TERMINATED", "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING", "TRIGGER", "TRUE", "TRUNCATE",
		"UNDO", "UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE", "USING", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP",
		"VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "VIRTUAL",
		"WHEN", "WHERE", "WHILE", "WITH", "WRITE",
		"XOR",
		"YEAR_MONTH",
		"ZEROFILL",
	}

	for _, keyword := range keywords {
		lowerKeyword := strings.ToLower(keyword)
		str = regexp.MustCompile(
			`\b`+lowerKeyword+`\b`).ReplaceAllString(str, keyword)
	}

	re := regexp.MustCompile(`\s+`)
	compressed := re.ReplaceAllString(str, " ")

	compressed = strings.TrimSpace(compressed)
	m := 96
	if len(compressed) <= m {
		return compressed
	}
	return compressed[:m] + "..."
}
