package queue

import (
	"log"
	"sync"
)

// DiskBackedQueue keeps Head in memory, tail spills over to disk
type DiskBackedQueue struct {
	HeadSizeLimit int
	Head          chan []byte
	Tail          *DiskByteQueue
	WriteChan     chan []byte
	Mutex         sync.Mutex // lock updates to Head
}

func NewDiskBackedQueue(dir, name string, inMemoryItemLimit int) (*DiskBackedQueue, error) {
	tail, err := NewDiskByteQueue(dir, name, false)
	q := &DiskBackedQueue{
		HeadSizeLimit: inMemoryItemLimit,
		Head:          make(chan []byte, inMemoryItemLimit),
		Tail:          tail,
		WriteChan:     make(chan []byte),
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
			}
		}
	}()
	return q, err
}

func (q *DiskBackedQueue) Destroy() {
	// println("destroying tail files")
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
