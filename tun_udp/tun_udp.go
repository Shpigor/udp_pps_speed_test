package main

import (
	"github.com/matishsiao/go_reuseport"
	"github.com/songgao/water"
	"github.com/vishvananda/netlink"
	"golang.org/x/net/ipv4"
	"log"
	"net"
	"os"
	"runtime"
	"sync"
	"syscall"
)

const (
	RCVR_BUF_SIZE int = 65536 // Possible to receive jumbo UDP packets
	RCVR_MSG_PACK int = 300   //Max number of messages in one ReadBatch call
)

const (
	tunName string = "tun1"
	txQLen  int    = 5000
	mtu     int    = 1500
)

var serverTunIP net.IP = []byte{10, 0, 1, 254}
var serverUDPIP = "192.168.7.38"
var serverUDPPort = "5110"

var clientTunIP net.IP = []byte{10, 0, 1, 1}
var clientUDPIP = "192.168.7.254"
var clientUDPPort = "5120"

type DataPacket struct {
	data      []byte
	packetLen int
}

type Batch struct {
	messages []ipv4.Message
	msgCount int
}

var packetPool *sync.Pool

var messagesPool *sync.Pool

var tunInterface *water.Interface
var udpListenConn *ipv4.PacketConn
var udpWriterConn net.PacketConn
var tunReadChan = make(chan *DataPacket, 100)
var udpReadChan = make(chan *DataPacket, 100)

var udpReadBatchChan = make(chan *Batch)

func initPool() {
	packetPool = &sync.Pool{
		New: func() interface{} {
			return &DataPacket{data: make([]byte, mtu), packetLen: 0}
		},
	}

	messagesPool = &sync.Pool{
		New: func() interface{} {
			return &Batch{getMessageBuffer(RCVR_MSG_PACK), 300}
		},
	}
}

func init() {
	initPool()
}

func getDataPacket() *DataPacket {
	return packetPool.Get().(*DataPacket)
}

func putDataPacket(p *DataPacket) {
	packetPool.Put(p)
}

func runTunReadThread() {
	go func() {
		runtime.LockOSThread()
		var packet = make([]byte, mtu)
		for {
			plen, err := tunInterface.Read(packet)
			if err != nil {
				log.Fatal("Tun Interface Read: type unknown %+v\n", err)
			}
			dataPacket := getDataPacket()
			copy(dataPacket.data, packet[:plen])
			dataPacket.packetLen = plen
			tunReadChan <- dataPacket
			//log.Printf("TUN packet received\n")
		}
	}()
}

func runTunWriteThread() {
	go func() {
		for pkt := range udpReadChan {
			_, err := tunInterface.Write(pkt.data[:pkt.packetLen])
			putDataPacket(pkt)
			if err != nil {
				log.Fatal("Tun Interface Write: type unknown %+v\n", err)
			}
			//log.Printf("TUN packet sent\n")
		}
	}()
}

func runTunWriteBatchThread() {
	go func() {
		for batch := range udpReadBatchChan {
			for i := 0; i < batch.msgCount; i++ {
				message := batch.messages[i]
				bytes := message.Buffers[0][:message.N]
				_, err := tunInterface.Write(bytes)
				if err != nil {
					log.Fatalf("Tun Interface Write: type unknown %+v\n", err)
				}
			}
			messagesPool.Put(batch)
			//log.Printf("TUN packet sent\n")
		}
	}()
}

func createTun(ip net.IP) {
	var tunNetwork = &net.IPNet{IP: ip, Mask: []byte{255, 255, 255, 0}}

	var config = water.Config{
		DeviceType: water.TUN,
		PlatformSpecificParams: water.PlatformSpecificParams{
			Name: tunName,
		},
	}

	var err error
	tunInterface, err = water.New(config)
	if nil != err {
		log.Fatal("Tun interface init(), Unable to allocate TUN interface: %+v\n", err)
	}

	link, err := netlink.LinkByName(tunName)
	if nil != err {
		log.Fatalf("Tun interface %s Up(), Unable to get interface info %+v\n", tunName, err)
	}
	err = netlink.LinkSetMTU(link, mtu)
	if nil != err {
		log.Fatalf("Tun interface %s Up() Unable to set MTU to %d on interface\n", tunName, mtu)

	}
	err = netlink.LinkSetTxQLen(link, txQLen)
	if nil != err {
		log.Fatalf("Tun interface %s Up() Unable to set MTU to %d on interface\n", tunName, mtu)
	}
	err = netlink.AddrAdd(link, &netlink.Addr{
		IPNet: tunNetwork,
		Label: "",
	})
	if nil != err {
		log.Fatalf("Tun interface %s Up() Unable to set IP to %s / %s on interface: %+v\n", tunName, tunNetwork.IP.String(), tunNetwork.String(), err)
	}

	err = netlink.LinkSetUp(link)
	if nil != err {
		log.Fatal("Tun interface Up() Unable to UP interface\n")
	}
	log.Printf("Tun interface %s Up() Tun(%s) interface with %s\n", tunName, tunNetwork.IP.String(), tunNetwork.String())
}

func runUDPReadThread() {
	go func() {
		runtime.LockOSThread()
		packet := make([]byte, mtu)
		for {
			plen, _, _, err := udpListenConn.ReadFrom(packet)
			if err != nil {
				log.Fatal("UDP Interface Read: type unknown %+v\n", err)
			}
			dataPacket := getDataPacket()
			copy(dataPacket.data, packet[:plen])
			dataPacket.packetLen = plen
			udpReadChan <- dataPacket
			//log.Printf("UDP packet received\n")
		}
	}()
}

func runUDPReadBatchThread() {
	go func() {
		runtime.LockOSThread()
		//packet := make([]byte, mtu)
		for {
			batch := messagesPool.Get().(*Batch)
			count, err := udpListenConn.ReadBatch(batch.messages, syscall.MSG_WAITFORONE)
			if err != nil {
				log.Fatal("UDP Interface Read: type unknown %+v\n", err)
			}
			batch.msgCount = count
			//log.Printf("%d UDP packet received\n", msgsCount)
			udpReadBatchChan <- batch
		}
	}()
}

func runUDPWriteThread(addrStr string) {
	addr, err := net.ResolveUDPAddr("", addrStr)
	if err != nil {
		log.Fatal("Unable to resolve UDP address %s: %+v\n", addrStr, err)
	}
	go func() {
		runtime.LockOSThread()
		for pkt := range tunReadChan {
			_, err := udpWriterConn.WriteTo(pkt.data[:pkt.packetLen], addr)
			putDataPacket(pkt)
			if err != nil {
				log.Fatal("UDP Interface Write: type unknown %+v\n", err)
			}
			//log.Printf("UDP packet sent\n")
		}
	}()
}

func createUDPListener(addrStr string) {
	laddr, errl := net.ResolveUDPAddr("udp4", addrStr)
	if errl != nil {
		log.Fatalf("Unable to open UDP listening socket for addr %s: %+v\n", addrStr, errl)
	}

	ln, err := net.ListenUDP("udp4", laddr)
	if err != nil {
		log.Fatalf("Unable to open UDP listening socket for addr %s: %+v\n", addrStr, err)
	}
	udpListenConn = ipv4.NewPacketConn(ln)
}

func createUDPWriter(addrStr string) {
	conn, err := reuseport.NewReusableUDPPortConn("udp", addrStr)
	if err != nil {
		log.Fatal("Unable to open UDP writing socket for addr %s: %+v\n", addrStr, err)
	}
	log.Printf("UDP writing conn: %s\n", addrStr)
	udpWriterConn = conn
}

func usageString() {
	log.Fatal("Usage: %s server|client\n", os.Args[0])
}

func getMessageBuffer(size int) []ipv4.Message {
	ms := make([]ipv4.Message, size)
	for i := 0; i < size; i++ {
		ms[i] = ipv4.Message{
			OOB:     make([]byte, 10),
			Buffers: [][]byte{make([]byte, RCVR_BUF_SIZE)},
		}
	}
	return ms
}

func main() {
	argc := len(os.Args)
	if argc < 2 {
		usageString()
	}

	switch os.Args[1] {
	case "server":
		createTun(serverTunIP)
		createUDPListener(serverUDPIP + ":" + serverUDPPort)
		createUDPWriter(serverUDPIP + ":" + clientUDPPort)
		runTunReadThread()
		runUDPReadThread()
		runUDPWriteThread(clientUDPIP + ":" + clientUDPPort)
		runTunWriteThread()
	case "client":
		createTun(clientTunIP)
		createUDPListener(clientUDPIP + ":" + clientUDPPort)
		createUDPWriter(clientUDPIP + ":" + serverUDPPort)
		runTunReadThread()
		runUDPReadThread()
		runUDPWriteThread(serverUDPIP + ":" + serverUDPPort)
		runTunWriteThread()
	default:
		usageString()
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
