package sender

import (
	"io"

	"github.com/chrislusf/netchan/service_discovery/client"
	"github.com/chrislusf/netchan/util"
)

func NewChannel(name string, leader string) (chan []byte, error) {
	ss := NewSenderServer()
	ss.Init()

	ch := make(chan []byte, 1000)
	ss.Handler = func(in io.Reader, out io.WriteCloser) {
		buf := make([]byte, 4)
		for data := range ch {
			util.Uint32toBytes(buf, uint32(len(data)))
			out.Write(buf)
			out.Write(data)
		}
	}

	go ss.Loop()

	b := client.NewHeartBeater(name, ss.Port, leader)
	go b.Start()

	return ch, nil
}
