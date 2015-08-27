package queue

import (
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestDiskByteQueue(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	dqName := "test_disk_byte_queue" + strconv.Itoa(int(time.Now().Unix()))
	dq, _ := NewDiskByteQueue(".", dqName, false)
	assert.NotEqual(t, dq, nil)

	for i := 0; i < 5; i++ {
		msg := []byte("test")
		dq.Enqueue(msg)
	}
	assert.Equal(t, dq.inCounter, 5)
	assert.Equal(t, dq.outCounter, 0)

	data, _ := dq.Dequeue()
	assert.Equal(t, string(data), "test")
	data, _ = dq.Dequeue()
	assert.Equal(t, string(data), "test")
	data, _ = dq.Dequeue()
	assert.Equal(t, string(data), "test")
	assert.Equal(t, dq.inCounter, 0)
	assert.Equal(t, dq.outCounter, 2)

	for i := 0; i < 4; i++ {
		msg := []byte("test")
		dq.Enqueue(msg)
	}
	assert.Equal(t, dq.inCounter, 4)
	assert.Equal(t, dq.outCounter, 2)

	data, _ = dq.Dequeue()
	assert.Equal(t, string(data), "test")
	data, _ = dq.Dequeue()
	assert.Equal(t, string(data), "test")
	assert.Equal(t, dq.inCounter, 4)
	assert.Equal(t, dq.outCounter, 0)

	data, _ = dq.Dequeue()
	assert.Equal(t, string(data), "test")
	assert.Equal(t, dq.inCounter, 0)
	assert.Equal(t, dq.outCounter, 3)

	dq.Destroy()

}
