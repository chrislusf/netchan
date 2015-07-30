package main

import (
	"flag"
	"strconv"

	"github.com/chrislusf/netchan/receiver"
	"github.com/chrislusf/netchan/sender"
	"github.com/chrislusf/netchan/service_discovery/leader"
)

var (
	name = flag.String("name", "worker", "a service name")
)

func main() {
	flag.Parse()

	// start a leader for service discovery
	go func() {
		leader.NewLeader(":8930")
	}()

	sendChan, err := sender.NewChannel("source", "127.0.0.1:8930")
	if err != nil {
		panic(err)
	}

	recvChan, err := receiver.NewChannel("source", "127.0.0.1:8930")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 100; i++ {
		sendChan <- []byte("message " + strconv.Itoa(i))
	}

	println("100 messages sent")

	for i := 0; i < 3; i++ {
		m := <-recvChan
		println(string(m))
	}

}
