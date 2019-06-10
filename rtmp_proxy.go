package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/monstarnn/rtmp_proxy/rtmp"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
)

func connectionLogger(data chan []byte, conn_n int, local_info, remote_info string) {
	logName := fmt.Sprintf("log-%s-%04d-%s-%s.log", formatTime(time.Now()),
		conn_n, local_info, remote_info)
	logger(data, logName)
}

func binaryLogger(data chan []byte, conn_n int, peer string) {
	logName := fmt.Sprintf("log-binary-%s-%04d-%s.log", formatTime(time.Now()),
		conn_n, peer)
	logger(data, logName)
}

func logger(data chan []byte, log_name string) {
	f, err := os.Create(log_name)
	if err != nil {
		logrus.Panicf("Unable to create file %s, %v\n", log_name, err)
	}
	defer f.Close()
	for {
		b := <-data
		if len(b) == 0 {
			break
		}
		f.Write(b)
		f.Sync()
	}
}

func formatTime(t time.Time) string {
	return t.Format("2006.01.02-15.04.05")
}

func printableAddr(a net.Addr) string {
	return strings.Replace(a.String(), ":", "-", -1)
}

type Channel struct {
	from, to             net.Conn
	logger, binaryLogger chan []byte
	ack                  chan bool
}

type ChannelRTMP struct {
	from, to             *rtmp.Conn
	fromURI, toURI       string
	logger, binaryLogger chan []byte
	ack                  chan bool
	comment              string
}

func passThrough(c *Channel, process ...func([]byte) (bool, []byte, error)) {
	fromPeer := printableAddr(c.from.LocalAddr())
	toPeer := printableAddr(c.to.LocalAddr())

	logrus.Infof("Starting pass through...\n")

	b := make([]byte, 10240)
	offset := 0
	packetN := 0
	for {
		n, err := c.from.Read(b)
		if err != nil {
			c.logger <- []byte(fmt.Sprintf("Disconnected from %s\n", fromPeer))
			break
		}
		if n > 0 {
			c.logger <- []byte(fmt.Sprintf("Received (#%d, %08X) %d bytes from %s\n",
				packetN, offset, n, fromPeer))
			data := b[:n]
			c.logger <- []byte(hex.Dump(data))
			if len(process) > 0 {
				var changed bool
				for _, p := range process {
					ch, replaced, chErr := p(data)
					if chErr != nil {
						c.logger <- []byte(fmt.Sprintf("Process data error: %s\n", chErr.Error()))
						break
					}
					if ch {
						data = replaced
						changed = true
					}
				}
				if changed {
					c.logger <- []byte(fmt.Sprintf("Changed (processed) data\n"))
					c.logger <- []byte(hex.Dump(data))
				}
			}
			c.binaryLogger <- data
			c.to.Write(data)
			c.logger <- []byte(fmt.Sprintf("Sent (#%d) to %s\n", packetN, toPeer))
			offset += n
			packetN += 1
		}
	}
	c.from.Close()
	c.to.Close()
	c.ack <- true
}

func passThroughRTMP(c *ChannelRTMP) {
	from_peer := printableAddr(c.from.NetConn().LocalAddr())
	to_peer := printableAddr(c.to.NetConn().LocalAddr())

	logrus.Infof("Starting pass through %s (%s -> %s)...", c.comment, from_peer, to_peer)

	//b := make([]byte, 10240)
	//offset := 0
	//packetN := 0

	//defer func(){
	//	c.from.Close()
	//	c.to.Close()
	//	c.ack <- true
	//}()

	//logrus.Println("!!!!!! c.to.Streams", c.comment)
	//if streams, err := c.to.Streams(); err != nil {
	//	logrus.Errorf("to.Streams %s error: %v", c.comment, err)
	//	return
	//} else {
	//	logrus.Println(streams)
	//}
	//
	logrus.Println("!!!!!! c.from.Streams", c.comment)
	if streams, err := c.from.Streams(); err != nil {
		logrus.Errorf("from.Streams %s error: %v", c.comment, err)
		return
	} else {
		logrus.Println(streams)
	}

	//if err := c.from.Prepare(); err != nil {
	//	logrus.Errorf("from.Prepare %s error: %v", c.comment, err)
	//	return
	//}

	for {
		if packet, err := c.from.ReadPacket(); err != nil {
			logrus.Errorf("from.ReadPacket %s error: %v", c.comment, err)
			return
		} else {
			logrus.Println("!!!! packet", packet)
		}
	}

	//for {
	//
	//	n, err := c.from.NetConn().Read(b)
	//	if err != nil {
	//		c.logger <- []byte(fmt.Sprintf("Disconnected from %s\n", from_peer))
	//		break
	//	}
	//	if n > 0 {
	//		c.logger <- []byte(fmt.Sprintf("Received (#%d, %08X) %d bytes from %s\n",
	//			packetN, offset, n, from_peer))
	//		c.logger <- []byte(hex.Dump(b[:n]))
	//		c.binaryLogger <- b[:n]
	//
	//		c.to.NetConn().Write(b[:n])
	//		c.logger <- []byte(fmt.Sprintf("Sent (#%d) to %s\n", packetN, to_peer))
	//		offset += n
	//		packetN += 1
	//	}
	//}
}

type replacer []byte

func (r replacer) searchAndReplace(search []byte, stepAfterSearch int, stop, replaceTo []byte) (result, found []byte) {

	var searchIndex int
	if searchIndex = bytes.Index(r, search); searchIndex == -1 {
		result = r
		return
	}

	//logrus.Println(hex.Dump(r[3:7]))
	//logrus.Println(binary.BigEndian.Uint32(r[3:7]), len(r)-12)

	searchIndex += len(search) + stepAfterSearch
	f := r[searchIndex:]
	if foundEnd := bytes.Index(f, stop); foundEnd != -1 {
		f = f[:foundEnd]
	}

	found = make([]byte, len(f))
	copy(found, f)
	//logrus.Println(hex.Dump(found))

	result = make([]byte, searchIndex)
	copy(result, r[:searchIndex])
	result = append(result, replaceTo...)
	result = append(result, r[searchIndex+len(found):]...)

	// set size
	result[searchIndex-1] = byte(len(replaceTo))

	return
}

func (r replacer) trimLeadZeros() (trimmed bool, result []byte) {
	for {
		if len(r) <= 16 {
			break
		}
		if searchIndex := bytes.Index(r, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}); searchIndex != 0 {
			break
		}
		trimmed = true
		r = r[16:]
	}
	result = r
	return
}

func (r replacer) setSize() {
	if len(r) < 7 {
		return
	}
	r[6] = byte(len(r) - 12)
	return
}

func processTcUrl(b []byte) (changed bool, replaced []byte, err error) {
	var tcUrl string
	var f []byte
	var toTcUrl = "rtmp://localhost:9999/live"
	if b, f = replacer(b).searchAndReplace([]byte("tcUrl"), 3, []byte{0}, []byte(toTcUrl)); f != nil {
		tcUrl = string(f)
		changed = true
	}
	if b, f = replacer(b).searchAndReplace([]byte("tcurl"), 3, []byte{0}, []byte(toTcUrl)); f != nil {
		tcUrl = string(f)
		changed = true
	}
	if changed {
		replaced = b
		logrus.Printf("tcUrl: %s -> %s", tcUrl, toTcUrl)
	}
	return
}

func processApp(b []byte) (changed bool, replaced []byte, err error) {

	var app string
	var f []byte
	var toApp = "live"
	if b, f = replacer(b).searchAndReplace([]byte("app"), 3, []byte{0}, []byte(toApp)); f != nil {
		app = string(f)
		changed = true
	}
	if changed {
		replaced = b
		logrus.Printf("app: %s -> %s", app, toApp)
	}
	return

}

func processTcUrlAndApp(b []byte) (changed bool, replaced []byte, err error) {
	if b[0] != byte(3) {
		return
	}
	var ch bool
	if changed, replaced, err = processTcUrl(b); err != nil {
		return
	} else {
		ch = true
		b = replaced
	}
	if changed, replaced, err = processApp(b); err != nil {
		return
	} else {
		ch = true
		b = replaced
	}
	if ch {
		_, replaced = replacer(b).trimLeadZeros()
		replacer(replaced).setSize()
	}
	return
}

func processRawConnection(local net.Conn, connN int, target string) {

	remote, err := net.Dial("tcp", target)
	if err != nil {
		logrus.Errorf("Unable to connect to %s, %v\n", target, err)
		return
	}

	localInfo := printableAddr(remote.LocalAddr())
	remoteInfo := printableAddr(remote.RemoteAddr())

	started := time.Now()

	logger := make(chan []byte)
	loggerFrom := make(chan []byte)
	loggerTo := make(chan []byte)

	ack := make(chan bool)

	go connectionLogger(logger, connN, localInfo, remoteInfo)
	go binaryLogger(loggerFrom, connN, localInfo)
	go binaryLogger(loggerTo, connN, remoteInfo)

	logger <- []byte(fmt.Sprintf("Connected to %s at %s\n", target,
		formatTime(started)))

	go passThrough(
		&Channel{
			local,
			remote,
			logger,
			loggerFrom,
			ack,
		},
		processTcUrlAndApp,
	)
	go passThrough(&Channel{
		remote,
		local,
		logger,
		loggerTo,
		ack,
	})

	<-ack
	<-ack

	finished := time.Now()
	duration := finished.Sub(started)
	logger <- []byte(fmt.Sprintf("Finished at %s, duration %s\n",
		formatTime(started), duration.String()))

	logger <- []byte{}
	loggerFrom <- []byte{}
	loggerTo <- []byte{}
}

func processConnectionRTMP(localConn net.Conn, connN int, target, targetURI string) {

	local := rtmp.NewConn(localConn)
	local.URL = new(url.URL)

	remote, err := rtmp.Dial(target + targetURI)
	if err != nil {
		logrus.Errorf("Unable to connect to %s, %v", target, err)
		return
	}

	localInfo := printableAddr(remote.NetConn().LocalAddr())
	remoteInfo := printableAddr(remote.NetConn().RemoteAddr())

	localURI := ""
	remoteURI := targetURI

	started := time.Now()

	logger := make(chan []byte)
	loggerFrom := make(chan []byte)
	loggerTo := make(chan []byte)

	ack := make(chan bool)

	go connectionLogger(logger, connN, localInfo, remoteInfo)
	go binaryLogger(loggerFrom, connN, localInfo)
	go binaryLogger(loggerTo, connN, remoteInfo)

	logger <- []byte(fmt.Sprintf("Connected to %s at %s\n", target,
		formatTime(started)))

	go passThroughRTMP(&ChannelRTMP{
		local,
		remote,
		localURI,
		remoteURI,
		logger,
		loggerFrom,
		ack,
		"local->remote",
	})
	go passThrough(&Channel{
		remote.NetConn(),
		local.NetConn(),
		logger,
		loggerTo,
		ack,
	})

	<-ack
	<-ack

	finished := time.Now()
	duration := finished.Sub(started)
	logger <- []byte(fmt.Sprintf("Finished at %s, duration %s\n",
		formatTime(started), duration.String()))

	logger <- []byte{}
	loggerFrom <- []byte{}
	loggerTo <- []byte{}
}

func main() {
	//runtime.GOMAXPROCS(runtime.NumCPU())
	var host, port, listen_port = "localhost", "1935", "9999"
	var target = net.JoinHostPort(host, port)
	var targetURI = "/live"
	logrus.Infof("Start listening on port %s and forwarding data to %s\n",
		listen_port, target)

	ln, err := net.Listen("tcp", ":"+listen_port)
	if err != nil {
		logrus.Errorf("Unable to start listener, %v\n", err)
		os.Exit(1)
	}
	conn_n := 1
	for {
		if conn, err := ln.Accept(); err == nil {
			lnn := ln.(*net.TCPListener)
			logrus.Infof("Accepted addr: %s, network: %s", lnn.Addr().String(), lnn.Addr().Network())
			if false {
				go processConnectionRTMP(conn, conn_n, target, targetURI)
			} else {
				go processRawConnection(conn, conn_n, target)
			}
			conn_n += 1
		} else {
			logrus.Errorf("Accept failed, %v\n", err)
		}
	}
}
