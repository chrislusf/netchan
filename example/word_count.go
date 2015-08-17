package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
)

var ()

type Task struct {
	Id     int
	Input  *DatasetShard
	Output *DatasetShard
	Step   *Step
}
type Step struct {
	Id       int
	Input    *Dataset
	Output   *Dataset
	Function func(*Task)
	Tasks    []*Task
}

type FlowContext struct {
	Steps []*Step
}

func (f *FlowContext) AddStep(input *Dataset, output *Dataset) (s *Step) {
	s = &Step{Input: input, Output: output, Id: len(f.Steps)}
	for i, shard := range input.Shards {
		t := &Task{Input: shard, Output: output.Shards[i], Step: s, Id: i}
		s.Tasks = append(s.Tasks, t)
	}
	f.Steps = append(f.Steps, s)
	return
}

type Dataset struct {
	context *FlowContext
	Type    reflect.Type
	Shards  []*DatasetShard

	ErrorChan chan error
	Generator func()
}

type DatasetShard struct {
	Parent    *Dataset
	ReadChan  chan reflect.Value
	WriteChan reflect.Value
}

func NewDataset(context *FlowContext, t reflect.Type) *Dataset {
	return &Dataset{
		context:   context,
		Type:      t,
		ErrorChan: make(chan error, 0),
	}
}

func (d *Dataset) Shard(n int) *Dataset {
	ctype := reflect.ChanOf(reflect.BothDir, d.Type)
	for i := 0; i < n; i++ {
		ds := &DatasetShard{
			Parent:    d,
			ReadChan:  make(chan reflect.Value, 0),
			WriteChan: reflect.MakeChan(ctype, 0),
		}
		d.Shards = append(d.Shards, ds)
	}
	return d
}

func (d *Dataset) addStep(taskFuncValue reflect.Value,
	taskExecution func(input reflect.Value, outChan reflect.Value),
) (ret *Dataset) {
	t := taskFuncValue.Type()
	if d.Type != t.In(0) {
		panic(fmt.Sprintf("The input %v does not match function input %v", d.Type, t.In(0)))
	}

	ret = NewDataset(d.context, d.Type)
	ret.Shard(len(d.Shards))

	step := d.context.AddStep(d, ret)
	step.Function = func(task *Task) {
		outChan := task.Output.WriteChan
		for input := range task.Input.ReadChan {
			taskExecution(input, outChan)
		}
		outChan.Close()
	}
	return
}

// f(A, chan B)
// input, type is same as parent Dataset's type
// output chan, element type is same as current Dataset's type
func (d *Dataset) Map(f interface{}) *Dataset {
	fn := reflect.ValueOf(f)
	return d.addStep(fn, func(input reflect.Value, outChan reflect.Value) {
		fn.Call([]reflect.Value{input, outChan})
	})
}

// f(A)bool
func (d *Dataset) Filter(f interface{}) *Dataset {
	fn := reflect.ValueOf(f)
	return d.addStep(fn, func(input reflect.Value, outChan reflect.Value) {
		outs := fn.Call([]reflect.Value{input})
		if outs[0].Bool() {
			outChan.Send(input)
		}
	})
}

func main() {
	flag.Parse()

	TextFile("/etc/passwd", 3).Map(func(line string, ch chan string) {
		ch <- line
	}).Filter(func(line string) bool {
		return !strings.HasPrefix(line, "#")
	}).Map(func(line string, ch chan string) {
		println(line)
		/*}).Partition(func(line string) int {
		i++
		return i*/
	}).Run()

}

func TextFile(fname string, shard int) (ret *Dataset) {
	ret = NewDataset(&FlowContext{}, reflect.TypeOf(""))
	ret.Shard(shard)
	ret.Generator = func() {
		// println("generate", fname)
		file, err := os.Open(fname)
		if err != nil {
			ret.ErrorChan <- err
			return
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		var i int
		for scanner.Scan() {
			ret.Shards[i].WriteChan.Send(reflect.ValueOf(scanner.Text()))
			i++
			if i == shard {
				i = 0
			}
		}

		for _, s := range ret.Shards {
			// println("closing source shard", i, "w")
			s.WriteChan.Close()
		}

		if err := scanner.Err(); err != nil {
			ret.ErrorChan <- err
			return
		}
	}
	return
}

func (d *Dataset) Run() {
	var wg sync.WaitGroup

	// start all task edges
	for i, step := range d.context.Steps {
		if i == 0 {
			wg.Add(1)
			go func(step *Step) {
				defer wg.Done()
				// println("start dataset", step.Id)
				step.Input.RunSelf(step.Id)
			}(step)
		}
		wg.Add(1)
		go func(step *Step) {
			defer wg.Done()
			step.Run()
		}(step)
		wg.Add(1)
		go func(step *Step) {
			defer wg.Done()
			// println("start dataset", step.Id+1)
			step.Output.RunSelf(step.Id + 1)
		}(step)
	}
	wg.Wait()
}

func (d *Dataset) RunSelf(stepId int) {
	var wg sync.WaitGroup
	for shardId, shard := range d.Shards {
		wg.Add(1)
		go func(shardId int, shard *DatasetShard) {
			defer wg.Done()
			var t reflect.Value
			for ok := true; ok; {
				if t, ok = shard.WriteChan.Recv(); ok {
					// fmt.Printf("%s -> r\n", t)
					shard.ReadChan <- t
				}
			}
			// println("dataset", stepId, "shard", shardId, "close r")
			close(shard.ReadChan)
		}(shardId, shard)
	}
	if d.Generator != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.Generator()
		}()
	}
	wg.Wait()
	// println("dataset", stepId, "stopped")
	return
}

func (s *Step) Run() {
	var wg sync.WaitGroup
	for _, t := range s.Tasks {
		wg.Add(1)
		go func(t *Task) {
			defer wg.Done()
			t.Run()
		}(t)
	}
	wg.Wait()
	return
}

// source ->w:ds:r -> task -> w:ds:r
// source close next ds' w chan
// ds close its own r chan
// task close next ds' w:ds

func (t *Task) Run() {
	// println("run step", t.Step.Id, "task", t.Id)
	t.Step.Function(t)
}
