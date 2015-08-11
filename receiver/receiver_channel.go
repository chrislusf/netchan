package receiver

import (
	"io"
	"log"
	"net"
	"time"

	"github.com/chrislusf/netchan/service_discovery/client"
	"github.com/chrislusf/netchan/util"
)

// keep alive by sending heartbeat every 2 seconds
func NewChannel(name string, leader string) (chan []byte, error) {
	l := client.NewNameServiceAgent(leader)

	ch := make(chan []byte)
	go func() {
		for {
			var target string
			for {
				locations := l.Find(name)
				if len(locations) > 0 {
					target = locations[0]
				}
				if target != "" {
					break
				} else {
					time.Sleep(time.Second)
					// print("z")
				}
			}
			// println("checking target", target)

			receiveTopicFrom(name, target, ch)
		}
	}()

	return ch, nil
}

func receiveTopicFrom(topicName, target string, ch chan []byte) {
	// connect to a TCP server
	network := "tcp"
	raddr, err := net.ResolveTCPAddr(network, target)
	if err != nil {
		log.Printf("Fail to resolve %s:%v", target, err)
		return
	}

	// println("dial tcp", raddr.String())
	conn, err := net.DialTCP(network, nil, raddr)
	if err != nil {
		log.Printf("Fail to dial %s:%v", raddr, err)
		time.Sleep(time.Second)
		return
	}
	defer conn.Close()

	buf := make([]byte, 4)

	util.WriteBytes(conn, buf, []byte("GET "+topicName))

	util.WriteBytes(conn, buf, []byte("ok"))

	ticker := time.NewTicker(time.Millisecond * 1100)
	defer ticker.Stop()
	go func() {
		buf := make([]byte, 4)
		for range ticker.C {
			util.WriteBytes(conn, buf, []byte("ok"))
			// print(".")
		}
	}()

	for {
		data, err := util.ReadBytes(conn, buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("read error:%v", err)
			continue
		}
		// fmt.Printf("read data %d: %v\n", len(data), err)
		ch <- data
	}
}
