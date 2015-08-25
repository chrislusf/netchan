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
	name2Queue map[string]queue.BackendQueue
	wg         sync.WaitGroup

	l net.Listener
}

func NewAgentLocalServer(port int) *AgentLocalServer {
	return &AgentLocalServer{
		Port:       port,
		name2Queue: make(map[string]queue.BackendQueue),
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
	// fmt.Println("AgentLocalServer starts on:", r.Port)
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
	// println("read request flag:", f, "size", len(cmd))
	if err != nil {
		log.Printf("Failed to read command %s:%v", string(cmd), err)
	}
	if bytes.HasPrefix(cmd, []byte("PUT ")) {
		name := string(cmd[4:])
		r.handleWriteConnection(conn, name)
	} else if bytes.HasPrefix(cmd, []byte("GET ")) {
		name := string(cmd[4:])
		r.handleLocalReadConnection(conn, name)
	}

}

func (als *AgentLocalServer) handleWriteConnection(r io.Reader, name string) {
	q, ok := als.name2Queue[name]
	if !ok {
		// println("write q is ", q)
		als.name2Queue[name] = queue.NewDiskQueue(name+strconv.Itoa(als.Port), os.TempDir(), 1024*1024, 2500, 2*time.Second)
		q = als.name2Queue[name]

		//register stream
		go client.NewHeartBeater(name, als.Port, "localhost:8930").Start()
	}

	counter := 0
	buf := make([]byte, 4)
	for {
		f, data, err := util.ReadBytes(r, buf)
		if err == io.EOF {
			q.Put([]byte{byte(util.FullStop)})
			// println("agent recv1 F->q:", "data size:", len(data))
			break
		}
		if f != util.Data {
			q.Put([]byte{byte(f)})
			// println("agent recv2 F->q:", "data size:", len(data))
			break
		}
		if err == nil {
			counter++
			q.Put([]byte{byte(f)})
			q.Put(data)
			// println("agent recv D:", string(data))
		}
	}
}

func (als *AgentLocalServer) handleLocalReadConnection(conn net.Conn, name string) {
	q, ok := als.name2Queue[name]
	if !ok {
		als.name2Queue[name] = queue.NewDiskQueue(name, os.TempDir(), 1024*1024, 2500, 2*time.Second)
		q = als.name2Queue[name]
	}

	closeSignal := make(chan bool)

	go func() {
		buf := make([]byte, 4)
		for {
			// println("wait for reader heartbeat")
			conn.SetReadDeadline(time.Now().Add(2500 * time.Millisecond))
			f, _, err := util.ReadBytes(conn, buf)
			if f != util.Data {
				closeSignal <- true
				return
			}
			if err != nil {
				// fmt.Printf("connection is closed? (%v)\n", err)
				closeSignal <- true
				return
			}
			// println("get reader heartbeat:", string(data))
		}
	}()

	counter := 0
	buf := make([]byte, 4)
	ch := q.ReadChan()
	closed := false
	for !closed {
		select {
		case flagBytes := <-ch:
			f := util.ControlFlag(flagBytes[0])
			if f == util.Data {
				data := <-ch
				util.WriteBytes(conn, util.Data, buf, data)
			} else {
				util.WriteBytes(conn, f, buf, nil)
				// println("agent send F: channel closing")
			}
			counter++
		case closed = <-closeSignal:
			// println("finishing handling connection")
			util.WriteBytes(conn, util.CloseChannel, buf, nil)
			// println("agent send F: channel closed")
			break
		}
	}
	// println("read connection is completed")
}
