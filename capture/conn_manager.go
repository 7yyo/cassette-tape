package capture

import (
	"hash/fnv"
	"runtime"
	"sync"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type connManager struct {
	routers    []*router
	connChan   chan *conn
	globalID   int
	cmMutex    sync.Mutex
	closeMutex sync.Mutex
}

func newConnManager() *connManager {

	size := routerSize()
	routers := make([]*router, size)
	for i := 0; i < size; i++ {
		routers[i] = newRouter(i)
	}
	cm := &connManager{
		connChan: make(chan *conn),
		routers:  routers,
	}
	go cm.closeWorker()
	return cm
}

var startStatisticsTimer = false

func (cm *connManager) push2conn(packet packet) {

	if !startStatisticsTimer {
		startStatisticsTimer = true
		go statisticsTimer()
	}

	from := packet.from
	index := hash(from, len(cm.routers))
	router := cm.routers[index]
	conns := router.conns

	var c *conn
	var exists bool
	if c, exists = conns[from]; !exists {
		cm.cmMutex.Lock()
		cm.globalID++
		cm.cmMutex.Unlock()

		var err error
		c, err = newConn(
			cm.globalID,
			router.index,
			from,
			cm.connChan,
		)
		if err != nil {
			log.Fatal("open file failed",
				zap.String("file", fileName),
				zap.Error(err),
			)
		}
		log.Debug("conn established",
			zap.Int("conn", c.id),
			zap.Int("router", router.index),
			zap.String("from", from),
		)
		CurrentConnCount.Add(1)
		router.conns[from] = c
		go c.run()
	}
	c.packetChan <- &packet
}

func (cm *connManager) closeWorker() {
	for conn := range cm.connChan {
		cm.closeMutex.Lock()
		conns := cm.routers[conn.router].conns
		delete(conns, conn.from)
		cm.closeMutex.Unlock()

		TotalCloseConnCount.Add(1)
		CurrentConnCount.Add(-1)
		log.Debug("conn closed",
			zap.Int("conn", conn.id),
			zap.Int("router", conn.router),
			zap.String("from", conn.from),
		)
	}
}

func hash(key string, size int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	hashValue := h.Sum32()
	return int(hashValue) % size
}

type router struct {
	index int
	conns map[string]*conn
}

func newRouter(i int) *router {
	return &router{
		index: i,
		conns: make(map[string]*conn),
	}
}

func routerSize() int {
	cpus := runtime.NumCPU() / 4
	if cpus == 0 {
		return 1
	}
	return cpus
}
