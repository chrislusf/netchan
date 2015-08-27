package queue

import ()

// BackendQueue represents the behavior for the secondary message
// storage system
type BackendQueue interface {
	Enqueue([]byte) error
	Dequeue() ([]byte, error)
	ReadChan() chan []byte
	Destroy()
	IsEmpty() bool
}
