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

type TaskOption struct {
	ContextId int
	StepId    int
	TaskId    int
}

func init() {
	var taskOption TaskOption
	flag.IntVar(&taskOption.ContextId, "task.context.id", -1, "context id")
	flag.IntVar(&taskOption.StepId, "task.step.id", -1, "step id")
	flag.IntVar(&taskOption.TaskId, "task.task.id", -1, "task id")

	flame.RegisterTaskRunner(NewTaskRunner(&taskOption))
}

type TaskRunner struct {
	option *TaskOption
	Task   *flame.Task
}

func NewTaskRunner(option *TaskOption) *TaskRunner {
	return &TaskRunner{option: option}
}

func (tr *TaskRunner) IsTaskMode() bool {
	return tr.option.TaskId >= 0 && tr.option.StepId >= 0 && tr.option.ContextId >= 0
}

func (tr *TaskRunner) ShouldRun(fc *flame.FlowContext, step *flame.Step, task *flame.Task) bool {
	return tr.option.TaskId == task.Id && tr.option.StepId == step.Id && tr.option.ContextId == fc.Id
}

// if this should not run, return false
func (tr *TaskRunner) Run(fc *flame.FlowContext, step *flame.Step, task *flame.Task) {
	tr.Task = task

	// 4. setup task input and output channels
	var wg sync.WaitGroup
	tr.connectInputs(&wg)
	tr.connectOutputs(&wg)
	// 6. starts to run the task locally
	tr.Task.Run()
	// 7. need to close connected output channels
	wg.Wait()
}

func (tr *TaskRunner) connectInputs(wg *sync.WaitGroup) {
	for _, shard := range tr.Task.Inputs {
		d := shard.Parent
		readChanName := fmt.Sprintf("ct-%d-ds-%d-shard-%d-", tr.option.ContextId, d.Id, shard.Id)
		// println("trying to read from:", readChanName)
		rawChan, err := GetReadChannel(readChanName)
		if err != nil {
			log.Panic(err)
		}
		shard.ReadChan = rawReadChannelToTyped(rawChan, d.Type, wg)
	}
}

func (tr *TaskRunner) connectOutputs(wg *sync.WaitGroup) {
	for _, shard := range tr.Task.Outputs {
		d := shard.Parent

		writeChanName := fmt.Sprintf("ct-%d-ds-%d-shard-%d-", tr.option.ContextId, d.Id, shard.Id)
		// println("writing to:", writeChanName)
		rawChan, err := GetSendChannel(writeChanName, wg)
		if err != nil {
			log.Panic(err)
		}
		connectTypedWriteChannelToRaw(shard.WriteChan, rawChan, wg)
	}
}

func rawReadChannelToTyped(c chan []byte, t reflect.Type, wg *sync.WaitGroup) chan reflect.Value {

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

		close(out)
	}()

	return out

}

func connectTypedWriteChannelToRaw(writeChan reflect.Value, c chan []byte, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		var t reflect.Value
		for ok := true; ok; {
			if t, ok = writeChan.Recv(); ok {
				var buf bytes.Buffer
				enc := gob.NewEncoder(&buf)
				if err := enc.EncodeValue(t); err != nil {
					log.Fatal("data type:", t.Kind(), " encode error:", err)
				}
				c <- buf.Bytes()
			}
		}
		close(c)

	}()

}
