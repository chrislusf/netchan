package queue

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sync"
)

// DiskByteQueue flips between 2 backing files.
// When reading head empties, writing tail flips to a new file.
type DiskByteQueue struct {
	Dir  string
	Name string

	filePrefix string
	inFile     *os.File
	outFile    *os.File

	inCounter  int
	outCounter int

	writeBuf bytes.Buffer

	inMux  sync.Mutex
	outMux sync.Mutex
}

func NewDiskByteQueue(dir string, name string, reuse bool) (q *DiskByteQueue, err error) {
	q = &DiskByteQueue{
		Dir:  dir,
		Name: name,
	}

	q.filePrefix = path.Join(dir, name)

	if !reuse {
		os.Remove(q.filePrefix + ".in")
		os.Remove(q.filePrefix + ".out")
	}

	if err = q.openFiles(); err != nil {
		return
	}

	return
}

func (q *DiskByteQueue) openFiles() (err error) {
	q.inFile, err = os.OpenFile(q.filePrefix+".in", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("Open Read error: %v", err)
	}
	q.outFile, err = os.OpenFile(q.filePrefix+".out", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("Open Write error: %v", err)
	}
	return
}

func (q *DiskByteQueue) Enqueue(data []byte) (err error) {
	q.inMux.Lock()
	defer q.inMux.Unlock()

	q.writeBuf.Reset()
	err = binary.Write(&q.writeBuf, binary.BigEndian, int32(len(data)))
	if err != nil {
		log.Printf("failed to write size to buffer: %v", err)
		return
	}

	_, err = q.writeBuf.Write(data)
	if err != nil {
		log.Printf("failed to add data to buffer: %v", err)
		return
	}

	_, err = q.inFile.Write(q.writeBuf.Bytes())
	if err != nil {
		log.Printf("Failed to write to %s: %v", q.inFile.Name(), err)
		return
	}
	q.inCounter++
	return
}

func (q *DiskByteQueue) Dequeue() (data []byte, err error) {
	q.outMux.Lock()
	defer q.outMux.Unlock()
	if q.outCounter <= 0 && q.inCounter > 0 {
		q.inMux.Lock()
		q.flipInOutFiles()
		q.inMux.Unlock()
	}
	if q.outCounter > 0 {
		var dataSize int32
		err = binary.Read(q.outFile, binary.BigEndian, &dataSize)
		if err != nil {
			q.outFile.Close()
			os.Remove(q.filePrefix + ".out")
			println("Failed to read data size:", err.Error())
			return
		}

		data = make([]byte, dataSize)
		_, err = io.ReadFull(q.outFile, data)
		if err != nil {
			q.outFile.Close()
			os.Remove(q.filePrefix + ".out")
		}
		// println("from tail:", dataSize, string(data))
		q.outCounter--
	}
	return
}

func (q *DiskByteQueue) flipInOutFiles() {
	os.Rename(q.filePrefix+".in", q.filePrefix+".out")
	q.openFiles()
	q.outCounter = q.inCounter
	q.inCounter = 0
	return
}

func (q *DiskByteQueue) Destroy() {
	q.inFile.Close()
	q.outFile.Close()
	if err := os.Remove(q.filePrefix + ".out"); err != nil {
		// println("Failed to remove out file", q.filePrefix+".out", err.Error())
	}
	if err := os.Remove(q.filePrefix + ".in"); err != nil {
		// println("Failed to remove in file", q.filePrefix+".in", err.Error())
	}
	return
}

func (q *DiskByteQueue) IsEmpty() bool {
	return q.inCounter == 0 && q.outCounter == 0
}

func (q *DiskByteQueue) Len() int {
	return q.inCounter + q.outCounter
}
