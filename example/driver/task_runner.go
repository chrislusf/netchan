package driver

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/chrislusf/netchan/example/flame"
)

type TaskContext struct {
	ContextId int
	StepId    int
	TaskId    int
}

var runner *TaskRunner
var taskContext TaskContext

func init() {
	flag.IntVar(&taskContext.ContextId, "task.context.id", -1, "context id")
	flag.IntVar(&taskContext.StepId, "task.step.id", -1, "step id")
	flag.IntVar(&taskContext.TaskId, "task.task.id", -1, "task id")

	runner = NewTaskRunner()
	flame.RegisterRunner(runner)
}

type TaskRunner struct {
	Task *flame.Task
}

func NewTaskRunner() *TaskRunner {
	return &TaskRunner{}
}

// if this should not run, return false
func (tr *TaskRunner) Run() bool {
	if taskContext.TaskId == -1 || taskContext.StepId == -1 || taskContext.ContextId == -1 {
		return false
	}
	// 1. setup connection to driver program
	// 2. receive the context
	// 3. find the task
	ctx := flame.Contexts[taskContext.ContextId]
	step := ctx.Steps[taskContext.StepId]
	tr.Task = step.Tasks[taskContext.TaskId]
	// 4. setup task inputs
	tr.connectInputs()
	// 5. setup task outputs
	tr.connectOutputs()
	// 6. starts to run the task locally
	tr.Task.Run()
	// 7. shutdown out channels

	return true
}

func (tr *TaskRunner) connectInputs() {
	for _, shard := range tr.Task.Inputs {
		d := shard.Parent
		readChanName := fmt.Sprintf("ds-%d-shard-%d-", d.Id, shard.Id)
		// println("trying to read from:", readChanName)
		rawChan, err := GetReadChannel(readChanName)
		if err != nil {
			log.Panic(err)
		}
		shard.ReadChan = rawReadChannelToTyped(rawChan, d.Type)
	}
}

func (tr *TaskRunner) connectOutputs() {
	for _, shard := range tr.Task.Outputs {
		d := shard.Parent

		writeChanName := fmt.Sprintf("ds-%d-shard-%d-", d.Id, shard.Id)
		// println("writing to:", writeChanName)
		rawChan, err := GetSendChannel(writeChanName)
		if err != nil {
			log.Panic(err)
		}
		connectTypedWriteChannelToRaw(shard.WriteChan, rawChan)
	}
}

func rawReadChannelToTyped(c chan []byte, t reflect.Type) chan reflect.Value {
	var wg sync.WaitGroup

	out := make(chan reflect.Value)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for data := range c {
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			v := reflect.New(t)
			if err := dec.DecodeValue(v); err != nil {
				log.Fatal("data type:", v.Kind(), "decode error:", err)
			} else {
				out <- reflect.Indirect(v)
			}
		}
	}()

	go func() {
		wg.Wait()
		close(out)
	}()
	return out

}

func connectTypedWriteChannelToRaw(writeChan reflect.Value, c chan []byte) {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		counter := 0
		var t reflect.Value
		for ok := true; ok; {
			if t, ok = writeChan.Recv(); ok {
				counter++
				println("sent", counter)
				var buf bytes.Buffer
				enc := gob.NewEncoder(&buf)
				if err := enc.EncodeValue(t); err != nil {
					log.Fatal("data type:", t.Kind(), " encode error:", err)
				}
				c <- buf.Bytes()
				println("sent", counter, ":", t.String())
			} else {
				println("sent closing....")
			}
		}
		println("sent closed.")

	}()

	go func() {
		wg.Wait()
		close(c)
	}()

}
