package agent

import (
	"log"
	"net"
	"strconv"
	"time"

	"github.com/chrislusf/netchan/queue"
	"github.com/chrislusf/netchan/util"
)

func (as *AgentServer) handleLocalReadConnection(conn net.Conn, name string) {
	as.name2QueueLock.Lock()
	ds, ok := as.name2Queue[name]
	if !ok {
		q, err := queue.NewDiskBackedQueue(as.dir, name+strconv.Itoa(as.Port), as.inMemoryItemLimit)
		if err != nil {
			// log.Printf("Failed to create queue on disk: %v", err)
			as.name2QueueLock.Unlock()
			return
		}
		as.name2Queue[name] = NewDataStore(q)
		ds = as.name2Queue[name]
	}
	as.name2QueueLock.Unlock()

	closeSignal := make(chan bool, 1)

	q := ds.Queue

	go func() {
		buf := make([]byte, 4)
		for {
			// println("wait for reader heartbeat")
			conn.SetReadDeadline(time.Now().Add(2500 * time.Millisecond))
			f, _, err := util.ReadBytes(conn, buf)
			if err != nil {
				// fmt.Printf("connection is closed? (%v)\n", err)
				closeSignal <- true
				close(closeSignal)
				return
			}
			if f != util.Data {
				closeSignal <- true
				close(closeSignal)
				return
			}
			// println("get", name, "heartbeat:", string(m.Bytes()))
			if len(q.Head) == 0 {
				q.Mutex.Lock()
				q.FillHead()
				q.Mutex.Unlock()
			} else {
				println(name, "head has", len(q.Head))
			}
		}
	}()

	buf := make([]byte, 4)

	// loop for every read
	done := false
	for !done {
		if len(q.Head) == 0 {
			q.Mutex.Lock()
			q.FillHead()
			q.Mutex.Unlock()
		}

		// println("tail:", q.Tail.Len(), "head:", len(q.Head))

		select {
		case messageBytes, ok := <-q.Head:
			if !ok {
				println(name, "read q head is already closed?")
				done = true
				break
			}
			if len(messageBytes) == 0 {
				// FIXME: how could it be possible? don't know yet
				log.Printf("%s Somehow this happens?", name)
				break
			}
			m := util.LoadMessage(messageBytes)
			// println(name, "send:", len(messageBytes), ":", string(m.Data()))
			util.WriteBytes(conn, buf, m)
		case <-closeSignal:
			// println("agent: client read connection closed")
			done = true
		}
	}

	as.name2QueueLock.Lock()
	if q.IsEmpty() {
		ds.Destroy()
		delete(as.name2Queue, name)
	}
	as.name2QueueLock.Unlock()
	// println("read connection is completed")
}
