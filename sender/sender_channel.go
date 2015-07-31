package sender

import (
	"io"
	"os"
	"time"

	"github.com/chrislusf/netchan/service_discovery/client"
	"github.com/chrislusf/netchan/util"
	"github.com/chrislusf/netchan/queue"
)

func NewChannel(name string, leader string) (chan []byte, error) {
	ss := NewSenderServer()
	ss.Init()

	dq := queue.NewDiskQueue(name, os.TempDir(), 1024*1024, 2500, 2*time.Second)
	ss.Handler = func(in io.Reader, out io.WriteCloser) {
		buf := make([]byte, 4)
		for data := range dq.ReadChan() {
			util.Uint32toBytes(buf, uint32(len(data)))
			out.Write(buf)
			out.Write(data)
		}
	}

	
	ch := make(chan []byte, 1000)
	go func(){
		for data := range ch {
			dq.Put(data)
		}
	}()

	go ss.Loop()

	b := client.NewHeartBeater(name, ss.Port, leader)
	go b.Start()

	return ch, nil
}
