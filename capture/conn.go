package capture

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"go.uber.org/zap"
)

type conn struct {
	id                  int
	router              int
	from                string
	packetChan          chan *packet
	connChan            chan *conn
	queryRecordChan     chan *QueryRecord
	nextSeq             uint32
	lastPacketTimestamp string
	buffer              *bytes.Buffer
	parser              *parser.Parser
	file                *os.File
	mutex               sync.Mutex
}

func newConn(id int, router int, from string, connChan chan *conn) (*conn, error) {
	c := &conn{
		id:              id,
		router:          router,
		from:            from,
		packetChan:      make(chan *packet, 1024),
		connChan:        connChan,
		queryRecordChan: make(chan *QueryRecord, 1024),
		parser:          parser.New(),
		mutex:           sync.Mutex{},
	}
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	c.file = f
	return c, nil
}

func (c *conn) run() {
	for {
		select {
		case packet := <-c.packetChan:
			c.analyze(*packet)
		case queryRecord := <-c.queryRecordChan:
			queryRecord.flush()
		}
	}
}

func (c *conn) analyze(p packet) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.nextSeq == 0 {
		c.nextSeq = p.seq + uint32(len(p.payload))
		c.buffer = bytes.NewBuffer(p.payload)
	} else {
		if p.seq == c.nextSeq {
			c.nextSeq = p.seq + uint32(len(p.payload))
			c.buffer.Write(p.payload)
		} else if p.seq > c.nextSeq {
			log.Debug(
				"sequence number discontinuity detected",
				zap.Int("conn", c.id),
				zap.Int("router", c.router),
				zap.Uint32("seq", p.seq),
				zap.Uint32("next", c.nextSeq),
			)
			TotalLostPacketCount.Add(1)
			c.buffer.Reset()
			c.buffer.Write(p.payload)
			c.nextSeq = p.seq + uint32(len(p.payload))
		} else {
			TotalOutOrderCount.Add(1)
			log.Debug(
				"out-of-order packet skipped",
				zap.Int("conn", c.id),
				zap.Int("router", c.router),
				zap.Uint32("seq", p.seq),
				zap.Uint32("next", c.nextSeq),
			)
		}
	}
	c.filterMySQLPacket()
}

func (c *conn) filterMySQLPacket() {
	data := c.buffer.Bytes()

	for len(data) >= 4 {
		length := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16

		if len(data) < int(length)+4 {
			return
		}

		mysqlPacket := data[4 : 4+length]

		if len(mysqlPacket) > 0 {
			c.setTimestamp()
			command := mysqlPacket[0]
			switch command {
			case 0x01:
				c.connChan <- c
			case 0x02, 0x04, 0x16, 0x17, 0x19, 0x8f:
			case 0x03:
				queries := []string{
					string(mysqlPacket[1:]),
				}
				qr := newQueryRecord(
					c.lastPacketTimestamp, c.id, c.router,
					c.from, queries, c.parser)
				qr.clean()
				err := qr.check()
				if err != nil {
					log.Warn("parse error",
						zap.String("sql", qr.queries[0]),
						zap.String("err", err.Error()))
					break
				}
				c.queryRecordChan <- qr
			default:
				log.Debug("unknown command",
					zap.Int("conn", c.id),
					zap.Int("router", c.router),
					zap.String("type", fmt.Sprintf("0x%02x", command)))
				UnknownCommandCount.Add(1)
			}
		}
		data = data[4+length:]
	}

	c.buffer.Reset()
	if len(data) > 0 {
		c.buffer.Write(data)
	}
}

func (c *conn) setTimestamp() {
	c.lastPacketTimestamp = time.Now().Format("2006-01-02 15:04:05")
}
