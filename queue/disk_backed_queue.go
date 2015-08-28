package queue

import (
	"log"
	"sync"
	"time"
)

// DiskBackedQueue keeps Head in memory, tail spills over to disk
type DiskBackedQueue struct {
	HeadSizeLimit int
	Head          chan []byte
	Tail          *DiskByteQueue
	WriteChan     chan []byte
	Mutex         sync.Mutex   // lock updates to Head
	ticker        *time.Ticker // FIXME: hack to push disk item to queue head sometimes
	killChan      chan bool
}

func NewDiskBackedQueue(dir, name string, inMemoryItemLimit int) (*DiskBackedQueue, error) {
	tail, err := NewDiskByteQueue(dir, name, false)
	q := &DiskBackedQueue{
		HeadSizeLimit: inMemoryItemLimit,
		Head:          make(chan []byte, inMemoryItemLimit),
		Tail:          tail,
		WriteChan:     make(chan []byte),
		ticker:        time.NewTicker(200 * time.Millisecond),
		killChan:      make(chan bool, 1),
	}

	go func() {
		for {
			select {
			case data, ok := <-q.WriteChan:
				if !ok {
					// println("done with writechan")
					return
				}
				q.Mutex.Lock()
				q.FillHead()
				if len(q.Head) < q.HeadSizeLimit && q.Tail.Len() == 0 {
					//short cut directly to head queue
					q.Head <- data
					// println("add to head", string(data))
				} else {
					// println("add to tail", string(data))
					q.Tail.Enqueue(data)
				}
				q.Mutex.Unlock()
			case _, ok := <-q.ticker.C:
				// move data to Head, without it, it will deadlock when
				// q.Tail has data, but q.Head is waiting
				if !ok {
					return
				}
				q.Mutex.Lock()
				q.FillHead()
				q.Mutex.Unlock()
			case <-q.killChan:
				return
			}
		}
	}()
	return q, err
}

func (q *DiskBackedQueue) Destroy() {
	q.killChan <- true
	q.ticker.Stop()
	q.Tail.Destroy()
}

func (q *DiskBackedQueue) IsEmpty() bool {
	return q.Len() <= 0
}

func (q *DiskBackedQueue) Len() int {
	return len(q.Head) + q.Tail.Len()
}

func (q *DiskBackedQueue) FillHead() {
	for len(q.Head) < q.HeadSizeLimit && !q.Tail.IsEmpty() {
		data, err := q.Tail.Dequeue()
		if err != nil {
			log.Printf("Tail %s error: %v", q.Tail.filePrefix, err)
			continue
		}
		q.Head <- data
		// println("adding data to queue head:", string(data))
	}
}
