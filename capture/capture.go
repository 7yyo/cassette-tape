package capture

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	snaplen = 65535
	promisc = true
	fName   = "Queries_%s.json"
)

var fileName string

type capture struct {
	device       string
	port         int
	packetSource *gopacket.PacketSource
	connManager  *connManager
	packetPool   sync.Pool
}

func newCapture(device string, port int, level string) (*capture, error) {

	switch level {
	case "info":
		log.SetLevel(zap.InfoLevel)
	case "debug":
		log.SetLevel(zap.DebugLevel)
	default:
		log.SetLevel(zap.InfoLevel)
	}

	return &capture{
		device:      device,
		port:        port,
		connManager: newConnManager(),
		packetPool: sync.Pool{
			New: func() any {
				return &packet{}
			},
		},
	}, nil
}

func (c *capture) run() error {

	err := c.newPacketSource()
	if err != nil {
		return err
	}

	err = createWriteBuffer()
	if err != nil {
		return fmt.Errorf("create writeBuffer failed: %w", err)
	}

	fmt.Println()
	fmt.Printf("🚀 Starting capture queries on interface %s from port %d\n\n", c.device, c.port)
	fmt.Println("⚠️ Prepare and execute can't be parsed, please set useServerPrepStmts=false")
	fmt.Println("⚠️ Please turn off SSL mode, like --ssl-mode=disabled, useSSL=false")
	fmt.Println()

	for p := range c.packetSource.Packets() {

		if p == nil {
			continue
		}

		tcpLayer := p.Layer(layers.LayerTypeTCP)
		if tcpLayer == nil {
			continue
		}
		tcp, _ := tcpLayer.(*layers.TCP)

		ipLayer := p.Layer(layers.LayerTypeIPv4)
		if ipLayer == nil {
			continue
		}
		ip, _ := ipLayer.(*layers.IPv4)
		if len(tcp.Payload) == 0 {
			continue
		}

		sourceIP := ip.SrcIP.String()
		sourcePort := tcp.SrcPort.String()

		packet := c.packetPool.Get().(*packet)

		packet.seq = tcp.Seq
		packet.from = net.JoinHostPort(sourceIP, sourcePort)
		packet.payload = tcp.Payload

		c.connManager.push2conn(*packet)

		c.packetPool.Put(packet)
	}
	return nil
}

func (c *capture) newPacketSource() error {
	handle, err := pcap.OpenLive(c.device, snaplen, promisc, pcap.BlockForever)
	if err != nil {
		return fmt.Errorf("opening device %s failed: %v\n", device, err)
	}
	filter := fmt.Sprintf("tcp port %d and dst port %d", c.port, c.port)
	err = handle.SetBPFFilter(filter)
	if err != nil {
		return fmt.Errorf("setting filter %s failed: %v\n", filter, err)
	}
	c.packetSource = gopacket.NewPacketSource(handle, handle.LinkType())
	return nil
}

func createWriteBuffer() error {
	fileName = fmt.Sprintf(fName, time.Now().Format("2006-01-02T15:04:05"))
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	bufWriter = bufio.NewWriterSize(f, 256*1024)
	return nil
}
