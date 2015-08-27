package driver

import (
	"flag"
	"sync"

	"github.com/chrislusf/netchan/receiver"
	"github.com/chrislusf/netchan/sender"
)

var (
	name = flag.String("name", "worker", "a service name")
)

type NetworkContext struct {
	LeaderAddress string
	AgentPort     int
}

var networkContext NetworkContext

func init() {
	flag.IntVar(&networkContext.AgentPort, "task.agent.port", 8931, "agent port")
	flag.StringVar(&networkContext.LeaderAddress, "task.leader.address", "localhost:8930", "leader address, as host:port")
}

func GetSendChannel(name string, wg *sync.WaitGroup) (chan []byte, error) {
	return sender.NewChannel(name, networkContext.AgentPort, wg)
}

func GetReadChannel(name string) (chan []byte, error) {
	return receiver.NewChannel(name, networkContext.LeaderAddress)
}
