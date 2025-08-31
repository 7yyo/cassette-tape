package capture

import (
	"bufio"
	"encoding/json"
	"strings"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/zap"
)

var writerMutex sync.Mutex
var bufWriter *bufio.Writer

type QueryRecord struct {
	Timestamp string `json:"timestamp"`
	Conn      int    `json:"conn"`
	router    int
	from      string
	Type      string `json:"type"`
	Digest    string `json:"digest"`
	Text      string `json:"text"`
	parser    *parser.Parser
	queries   []string
}

func newQueryRecord(
	timestamp string, conn, router int, from string, queries []string, parser *parser.Parser) *QueryRecord {
	return &QueryRecord{
		Timestamp: timestamp,
		Conn:      conn,
		router:    router,
		from:      from,
		queries:   queries,
		parser:    parser,
	}
}

func (qr *QueryRecord) check() error {
	query := qr.queries[0]
	stmts, _, err := qr.parser.Parse(query, "utf-8", "")
	if err != nil {
		ParseErrorCount.Add(1)
		return err
	}
	_, digest := parser.NormalizeDigest(query)
	for _, stmt := range stmts {
		switch stmt.(type) {
		case *ast.SelectStmt:
			qr.Type = "select"
		case *ast.InsertStmt:
			qr.Type = "insert"
		case *ast.UpdateStmt:
			qr.Type = "update"
		case *ast.DeleteStmt:
			qr.Type = "delete"
		case *ast.CommitStmt:
			qr.Type = "commit"
		case *ast.RollbackStmt:
			qr.Type = "rollback"
		case *ast.AlterTableStmt,
			*ast.AlterSequenceStmt,
			*ast.AlterPlacementPolicyStmt,
			*ast.AlterResourceGroupStmt,
			*ast.CreateDatabaseStmt,
			*ast.CreateIndexStmt,
			*ast.CreateTableStmt,
			*ast.CreateViewStmt,
			*ast.CreateSequenceStmt,
			*ast.CreatePlacementPolicyStmt,
			*ast.CreateResourceGroupStmt,
			*ast.DropDatabaseStmt,
			*ast.DropIndexStmt,
			*ast.DropTableStmt,
			*ast.DropSequenceStmt,
			*ast.DropPlacementPolicyStmt,
			*ast.DropResourceGroupStmt,
			*ast.OptimizeTableStmt,
			*ast.RenameTableStmt,
			*ast.TruncateTableStmt,
			*ast.RepairTableStmt:
			qr.Type = "ddl"
		case *ast.AnalyzeTableStmt:
			qr.Type = "analyze"
		default:
			qr.Type = "others"
		}
		qr.Digest = digest.String()
		TotalQueryCount.Add(1)
		qr.Text += stmt.Text() + ";"
	}
	return nil
}

func (qr *QueryRecord) flush() {

	jsonData, err := json.Marshal(qr)
	if err != nil {
		log.Warn("marshal failed", zap.Error(err))
		return
	}

	writerMutex.Lock()
	defer writerMutex.Unlock()

	if bufWriter == nil {
		log.Fatal("buffer writer not initialized")
	}
	buffer := make([]byte, len(jsonData)+1)
	copy(buffer, jsonData)
	buffer[len(jsonData)] = '\n'
	_, err = bufWriter.Write(buffer)
	if err != nil {
		log.Warn("write failed", zap.Error(err))
		return
	}
	err = bufWriter.Flush()
	if err != nil {
		log.Warn("flush buffer failed", zap.Error(err))
	}
}

func (qr *QueryRecord) clean() {
	queries := make([]string, 0, 1)
	for _, query := range qr.queries {
		cleaned := strings.Map(func(r rune) rune {
			if r < 32 && r != 9 && r != 10 && r != 13 {
				return -1
			}
			return r
		}, query)

		if cleaned != "" {
			queries = append(queries, cleaned)
		}
	}
	qr.queries = queries
}
