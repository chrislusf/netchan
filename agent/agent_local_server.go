// agent
package agent

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/chrislusf/netchan/queue"
	"github.com/chrislusf/netchan/service_discovery/client"
	"github.com/chrislusf/netchan/util"
)

type AgentLocalServer struct {
	Port       int
	name2Queue map[string]*queue.DiskBackedQueue
	wg         sync.WaitGroup

	l net.Listener
}

func NewAgentLocalServer(port int) *AgentLocalServer {
	return &AgentLocalServer{
		Port:       port,
		name2Queue: make(map[string]*queue.DiskBackedQueue),
	}
}

// Start starts to listen on a port, returning the listening port
// r.Port can be pre-set or leave it as zero
// The actual port set to r.Port
func (r *AgentLocalServer) Init() (err error) {
	r.l, err = net.Listen("tcp", ":"+strconv.Itoa(r.Port))
	if err != nil {
		log.Fatal(err)
	}

	r.Port = r.l.Addr().(*net.TCPAddr).Port
	fmt.Println("AgentLocalServer starts on:", r.Port)
	return
}

func (r *AgentLocalServer) Run() {
	for {
		// Listen for an incoming connection.
		conn, err := r.l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		r.wg.Add(1)
		go func() {
			defer conn.Close()
			r.handleRequest(conn)
		}()
	}
}

func (r *AgentLocalServer) Stop() {
	r.l.Close()
	r.wg.Wait()
}

// Handles incoming requests.
func (r *AgentLocalServer) handleRequest(conn net.Conn) {
	defer r.wg.Done()
	defer conn.Close()

	buf := make([]byte, 4)

	f, cmd, err := util.ReadBytes(conn, buf)
	if f != util.Data {
		//strange if this happens
		return
	}
	// println("read request flag:", f, "data", string(cmd.Data()))
	if err != nil {
		log.Printf("Failed to read command %s:%v", string(cmd.Data()), err)
	}
	if bytes.HasPrefix(cmd.Data(), []byte("PUT ")) {
		name := string(cmd.Data()[4:])
		r.handleWriteConnection(conn, name)
	} else if bytes.HasPrefix(cmd.Data(), []byte("GET ")) {
		name := string(cmd.Data()[4:])
		r.handleLocalReadConnection(conn, name)
	}

}

func (als *AgentLocalServer) handleWriteConnection(r io.Reader, name string) {
	q, ok := als.name2Queue[name]
	if !ok {
		var err error
		als.name2Queue[name], err = queue.NewDiskBackedQueue(".", name+strconv.Itoa(als.Port), 2)
		if err != nil {
			log.Printf("Failed to create a queue on disk: %v", err)
			return
		}
		q = als.name2Queue[name]

		//register stream
		go client.NewHeartBeater(name, als.Port, "localhost:8930").Start()
	}

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
			q.WriteChan <- message.Bytes()
			// println("agent recv:", string(message.Bytes()))
		}
	}
}

func (als *AgentLocalServer) handleLocalReadConnection(conn net.Conn, name string) {
	q, ok := als.name2Queue[name]
	if !ok {
		var err error
		als.name2Queue[name], err = queue.NewDiskBackedQueue(".", name+strconv.Itoa(als.Port), 2)
		if err != nil {
			log.Printf("Failed to create queue on disk: %v", err)
			return
		}
		q = als.name2Queue[name]
	}

	closeSignal := make(chan bool, 1)

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
			// println("get reader heartbeat:", string(data))
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
				// println("read q head is already closed?")
				done = true
				break
			}
			if len(messageBytes) == 0 {
				// FIXME: how could it be possible? don't know yet
				break
			}
			// println("from head:", len(messageBytes), ":", string(messageBytes))
			m := util.LoadMessage(messageBytes)
			util.WriteBytes(conn, buf, m)
		case <-closeSignal:
			// println("agent: client read connection closed")
			done = true
			break
		}
	}

	if q.IsEmpty() {
		q.Destroy()
		delete(als.name2Queue, name)
	}
	// println("read connection is completed")
}
