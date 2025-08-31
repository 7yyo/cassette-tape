package capture

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	TotalQueryCount      atomic.Int32
	CurrentConnCount     atomic.Int32
	TotalCloseConnCount  atomic.Int32
	TotalLostPacketCount atomic.Int32
	TotalOutOrderCount   atomic.Int32
	UnknownCommandCount  atomic.Int32
	ParseErrorCount      atomic.Int32

	startTime = time.Now()
)

func statisticsTimer() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		printStatistics()
	}
}

func printStatistics() {

	queryCount := TotalQueryCount.Load()
	currentConnCount := CurrentConnCount.Load()
	closeConnCount := TotalCloseConnCount.Load()
	lostPacketCount := TotalLostPacketCount.Load()
	outOrderCount := TotalOutOrderCount.Load()
	unknownCommandCount := UnknownCommandCount.Load()
	parseErrorCount := ParseErrorCount.Load()

	var qps float64
	elapsed := time.Since(startTime).Seconds()
	if elapsed > 0 {
		qps = math.Round(float64(queryCount)/elapsed*100) / 100
	}

	log.Info("",
		zap.Int32("queries", queryCount),
		zap.Int32("currentConn", currentConnCount),
		zap.Int32("closedConn", closeConnCount),
		zap.Int32("lost", lostPacketCount),
		zap.Int32("crossed", outOrderCount),
		zap.Int32("unknown", unknownCommandCount),
		zap.Int32("parseError", parseErrorCount),
		zap.Float64("qps", qps),
	)
}
