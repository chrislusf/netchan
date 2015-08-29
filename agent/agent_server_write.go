package agent

import (
	"io"
	"log"
	"strconv"

	"github.com/chrislusf/netchan/queue"
	"github.com/chrislusf/netchan/service_discovery/client"
	"github.com/chrislusf/netchan/util"
)

func (as *AgentServer) handleWriteConnection(r io.Reader, name string) {
	as.name2QueueLock.Lock()
	ds, ok := as.name2Queue[name]
	if !ok {
		q, err := queue.NewDiskBackedQueue(as.dir, name+strconv.Itoa(as.Port), as.inMemoryItemLimit)
		if err != nil {
			log.Printf("Failed to create a queue on disk: %v", err)
			as.name2QueueLock.Unlock()
			return
		}
		as.name2Queue[name] = NewDataStore(q)
		ds = as.name2Queue[name]

		//register stream
		go client.NewHeartBeater(name, as.Port, "localhost:8930").StartHeartBeat(ds.killHeartBeater)
	}
	as.name2QueueLock.Unlock()

	counter := 0
	buf := make([]byte, 4)
	// write chan is not closed, for writes from another request
	for {
		// f is already included in message
		_, message, err := util.ReadBytes(r, buf)
		if err == io.EOF {
			// println("agent recv eof:", string(message.Bytes()))
			break
		}
		if err == nil {
			counter++
			ds.Queue.WriteChan <- message.Bytes()
			// println("agent recv:", string(message.Bytes()))
		}
	}
}
