package receiver

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/chrislusf/netchan/service_discovery/client"
	"github.com/chrislusf/netchan/util"
)

func NewChannel(name string, leader string) (chan []byte, error) {
	l := client.NewNameServiceAgent(leader)
	var target string
	for {
		locations := l.Find(name)
		if len(locations) > 0 {
			target = locations[0]
			println("target:", target)
		}
		if target != "" {
			break
		}
	}
	fmt.Println("Hello World from Receiver:", target)

	// connect to a TCP server
	network := "tcp"
	raddr, err := net.ResolveTCPAddr(network, target)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.DialTCP(network, nil, raddr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to ", target)

	ch := make(chan []byte)

	go func() {
		defer conn.Close()
		buf := make([]byte, 4)
		for {
			c, err := io.ReadAtLeast(conn, buf, 4)
			if err != nil {
				log.Printf("%d:%v", c, err)
			}
			size := util.BytesToUint32(buf)
			fmt.Println("size", size)
			data := make([]byte, int(size))
			io.ReadAtLeast(conn, data, int(size))

			ch <- data
		}
	}()

	return ch, nil
}
