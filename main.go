package main

import (
	"github.com/mailru/easygo/netpoll"
	"github.com/matishsiao/go_reuseport"
	"github.com/tidwall/evio"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

// #include <sys/syscall.h>
import "C"

const (
	bb_payload_sz = 1048
	payload_sz    = 1024
	chunk_sz      = 1024
	local_port    = 5674
)

var packets, bytes int32

func main() {
	if len(os.Args) < 4 {
		usage()
	}

	addr, err := net.ResolveUDPAddr("", os.Args[3])
	chk(err)

	iter, err := strconv.Atoi(os.Args[2])
	chk(err)
	iter *= 1E6
	wg := sync.WaitGroup{}
	wg.Add(1)

	switch os.Args[1] {
	case "write":
		write(addr, iter)
	case "writeTo":
		writeTo(os.Args[3], int32(iter))
	case "writeToUDP":
		writeToUDP(addr, iter)
	case "sendTo":
		sendTo(addr, iter)
	case "sendMsg":
		sendMsg(addr, iter)
	case "sendMMsg":
		sendMMsg(addr, iter)
	case "listen":
		listenFDPoll(addr)
	case "listenB":
		listenBatch(addr)
	case "listenE":
		listenEvio(addr)
	case "All":
		write(addr, iter)
		time.Sleep(time.Second * 2)
		writeToUDP(addr, iter)
		time.Sleep(time.Second * 2)
		sendTo(addr, iter)
		time.Sleep(time.Second * 2)
		sendMsg(addr, iter)
		time.Sleep(time.Second * 2)
		sendMMsg(addr, iter)
	default:
		usage()
	}
	wg.Wait()
}

func usage() {
	log.Fatal("Usage: %s write|writeToUDP|sendTo|sendMsg|sendMMsg|All  iteration_count*1M  target_ip:port", os.Args[0])
}

func chk(err error) {
	if err != nil {
		panic(err)
	}
}

func write(addr *net.UDPAddr, i int) {
	log.Printf("Start `write` test with %d iteration\n", i)

	conn, err := net.DialUDP(addr.Network(), nil, addr)
	chk(err)

	payload := make([]byte, payload_sz)
	for ; i > 0; i-- {
		_, err := conn.Write(payload)
		chk(err)
	}
}

func writeTo(addrStr string, i int32) {
	log.Printf("Start `write` test with %d iteration\n", i)
	addr, err := net.ResolveUDPAddr("", addrStr)
	chk(err)
	log.Printf("addrStr: %s", addrStr)
	conn, err := reuseport.NewReusableUDPPortConn("udp", addrStr)
	chk(err)

	ch := make(chan []byte, 1000)
	go func(chan []byte) {
		for pl := range ch {
			_, err := conn.WriteTo(pl, addr)
			chk(err)
		}
	}(ch)

	for g := 0; g < 4; g++ {
		go func() {
			payload := make([]byte, payload_sz)
			for ; atomic.LoadInt32(&i) > 0; atomic.AddInt32(&i, -1) {
				ch <- payload
			}
		}()
	}
}

func writeToUDP(addr *net.UDPAddr, i int) {
	log.Printf("Start `writeToUDP` test with %d iteration\n", i)

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	chk(err)

	payload := make([]byte, payload_sz)
	for ; i > 0; i-- {
		_, err := conn.WriteToUDP(payload, addr)
		chk(err)
	}
}

func sendTo(addr *net.UDPAddr, i int) {
	log.Printf("Start `sendTo` test with %d iteration\n", i)

	laddr := UDPAddrToSockaddr(&net.UDPAddr{Port: local_port, IP: net.IPv4zero})
	raddr := UDPAddrToSockaddr(addr)

	fd := connectUDP(laddr, raddr)

	payload := make([]byte, payload_sz)
	for ; i > 0; i-- {
		err := syscall.Sendto(fd, payload, syscall.MSG_DONTWAIT, raddr)
		chk(err)
	}
}

func sendMsg(addr *net.UDPAddr, i int) {
	log.Printf("Start `sendMsg` test with %d iteration\n", i)

	laddr := UDPAddrToSockaddr(&net.UDPAddr{Port: local_port, IP: net.IPv4zero})
	raddr := UDPAddrToSockaddr(addr)

	fd := connectUDP(laddr, raddr)

	payload := make([]byte, payload_sz)
	for ; i > 0; i-- {
		err := syscall.Sendmsg(fd, payload, nil, raddr, syscall.MSG_DONTWAIT)
		chk(err)
	}
}

type MMsghdr struct {
	Msg syscall.Msghdr
	cnt int
}

func sendMMsg(addr *net.UDPAddr, i int) {
	i = i / chunk_sz
	log.Printf("Start `sendMMsg` test with %d iteration\n", i)

	laddr := UDPAddrToSockaddr(&net.UDPAddr{Port: local_port, IP: net.IPv4zero})
	raddr := UDPAddrToSockaddr(addr)

	fd := connectUDP(laddr, raddr)

	msgcnt := chunk_sz
	var msgArr [chunk_sz]MMsghdr
	for j := 0; j < msgcnt; j++ {
		p := make([]byte, payload_sz)
		for k := 0; k < payload_sz; k++ {
			p[k] = byte(k)
		}

		var iov syscall.Iovec
		iov.Base = (*byte)(unsafe.Pointer(&p[0]))
		iov.SetLen(len(p))

		var msg syscall.Msghdr
		msg.Iov = &iov
		msg.Iovlen = 1

		msgArr[j] = MMsghdr{msg, 0}
	}

	for ; i > 0; i-- {
		//_, _, e1 := syscall.Syscall6(C.SYS_sendmmsg, uintptr(fd), uintptr(unsafe.Pointer(&msgArr[0])), uintptr(msgcnt), uintptr(syscall.MSG_DONTWAIT), 0, 0)
		_, _, e1 := syscall.Syscall6(C.SYS_sendmmsg, uintptr(fd), uintptr(unsafe.Pointer(&msgArr[0])), uintptr(msgcnt), 0, 0, 0)
		if e1 != 0 {
			panic("error on sendmmsg")
		}
	}
}

func UDPAddrToSockaddr(addr *net.UDPAddr) *syscall.SockaddrInet4 {
	raddr := &syscall.SockaddrInet4{Port: addr.Port, Addr: [4]byte{addr.IP[12], addr.IP[13], addr.IP[14], addr.IP[15]}}
	return raddr
}

func listenFDPoll(srcUdpAddr *net.UDPAddr) {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		log.Print("Init ticker...")
		for range ticker.C {
			log.Printf("Received %2.3f packets/s and %2.3f MB/s", float32(atomic.SwapInt32(&packets, 0))/1000000, float32(atomic.SwapInt32(&bytes, 0))/1048576)
		}
	}()

	listConn, listErr := net.ListenUDP("udp", srcUdpAddr)
	chk(listErr)
	listErr = listConn.SetReadBuffer(bb_payload_sz)
	poll, pollErr := netpoll.New(nil)
	chk(pollErr)
	desc := netpoll.Must(netpoll.HandleRead(listConn))
	data := make([]byte, bb_payload_sz)
	eventErr := poll.Start(desc, func(event netpoll.Event) {
		size, readErr := listConn.Read(data)
		chk(readErr)
		if size <= bb_payload_sz {
			atomic.AddInt32(&packets, 1)
			atomic.AddInt32(&bytes, int32(size))
		} else {
			log.Print("Got more bytes rather than 32!!!")
		}
	})
	chk(eventErr)
}

func listenBatch(srcUdpAddr *net.UDPAddr) {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		log.Print("Init ticker...")
		for range ticker.C {
			log.Printf("Received %2.3f packets/s and %2.3f MB/s", float32(atomic.SwapInt32(&packets, 0))/1000000, float32(atomic.SwapInt32(&bytes, 0))/1048576)
		}
	}()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, 0)
	chk(err)
	err = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF|syscall.SO_REUSEADDR, payload_sz)
	chk(err)
	err = syscall.Bind(fd, UDPAddrToSockaddr(srcUdpAddr))
	chk(err)
	bb := make([]byte, 1048)
	for {
		n, _, _, _, err := syscall.Recvmsg(fd, bb, nil, syscall.MSG_WAITFORONE)
		chk(err)
		if n <= 1048 {
			atomic.AddInt32(&packets, int32(n/1024))
			atomic.AddInt32(&bytes, int32(n))
		} else {
			log.Print("Got more bytes rather than 32!!!")
		}
	}
}

func listenEvio(srcUdpAddr *net.UDPAddr) {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		log.Print("Init ticker...")
		for range ticker.C {
			log.Printf("Received %2.3f packets/s and %2.3f MB/s", float32(atomic.SwapInt32(&packets, 0))/1000000, float32(atomic.SwapInt32(&bytes, 0))/1048576)
		}
	}()
	var events evio.Events
	events.NumLoops = 2
	events.Opened = func(c evio.Conn) (out []byte, opts evio.Options, action evio.Action) {
		opts = evio.Options{ReuseInputBuffer: true}
		return
	}
	events.Data = func(c evio.Conn, in []byte) (out []byte, action evio.Action) {
		size := len(in)
		if size <= payload_sz {
			atomic.AddInt32(&packets, 1)
			atomic.AddInt32(&bytes, int32(size))
		} else {
			log.Print("Got more bytes rather than 32!!!")
		}
		return
	}

	if err := evio.Serve(events, "udp://192.168.7.30:4321?reuseport=true", "udp://127.0.0.1:4321?reuseport=true"); err != nil {
	}
}

func connectUDP(laddr, raddr syscall.Sockaddr) int {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, 0)
	chk(err)

	err = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	chk(err)

	err = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, payload_sz)
	chk(err)

	err = syscall.Bind(fd, laddr)
	chk(err)

	err = syscall.Connect(fd, raddr)
	chk(err)

	return fd
}
