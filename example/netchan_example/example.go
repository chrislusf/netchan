package main

import (
	"flag"
	"strconv"

	"github.com/chrislusf/netchan/agent"
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
		leader.RunLeader(":8930")
	}()

	go func() {
		agentServer := agent.NewAgentServer(8931, "localhost:8930")
		agentServer.Run()
	}()

	sendChan, err := sender.NewChannel("source", 8931)
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

	for m := range recvChan {
		println(string(m))
	}

}
