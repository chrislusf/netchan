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
	Inputs   []*Dataset
	Outputs  []*Dataset
	Function func()
}

func NewTask(inputs []*Dataset, outputs []*Dataset) *Task {
	return &Task{Inputs: inputs, Outputs: outputs}
}

type FlowContext struct {
	Tasks []*Task
}

type Dataset struct {
	Type      reflect.Type
	context   *FlowContext
	ReadChan  chan reflect.Value
	WriteChan reflect.Value
	ErrorChan chan error
	Parents   []*Dataset

	Generator func()
}

func NewDataset(context *FlowContext, t reflect.Type) *Dataset {
	ctype := reflect.ChanOf(reflect.BothDir, t)
	return &Dataset{
		context:   context,
		Type:      t,
		ReadChan:  make(chan reflect.Value, 0),
		WriteChan: reflect.MakeChan(ctype, 0),
		ErrorChan: make(chan error, 0),
	}
}

func (d *Dataset) DependsOn(parent *Dataset) {
	for _, p := range d.Parents {
		if p == parent {
			return
		}
	}
	d.Parents = append(d.Parents, parent)
}

// f(A, chan B)
func (d *Dataset) Map(f interface{}) (ret *Dataset) {
	t := reflect.TypeOf(f)
	if d.Type != t.In(0) {
		panic(fmt.Sprintf("The input %v does not match function input %v", d.Type, t.In(0)))
	}

	ret = NewDataset(d.context, t.In(1).Elem())
	ret.DependsOn(d)

	task := NewTask([]*Dataset{d}, []*Dataset{ret})
	task.Function = func() {
		fn := reflect.ValueOf(f)
		// input, type is same as parent Dataset's type
		// output chan, element type is same as current Dataset's type
		ch := task.Outputs[0].WriteChan
		// println("map run 1")
		for input := range task.Inputs[0].ReadChan {
			println("map run 2", input.String())
			fn.Call([]reflect.Value{input, ch})
		}
	}

	d.context.Tasks = append(d.context.Tasks, task)
	return
}

// f(A)bool
func (d *Dataset) Filter(f interface{}) (ret *Dataset) {
	t := reflect.TypeOf(f)
	if d.Type != t.In(0) {
		panic(fmt.Sprintf("The input %v does not match function input %v", d.Type, t.In(0)))
	}

	ret = NewDataset(d.context, t.In(0))
	ret.DependsOn(d)

	task := NewTask([]*Dataset{d}, []*Dataset{ret})
	task.Function = func() {
		fn := reflect.ValueOf(f)
		// println("filter run 1")
		outChan := task.Outputs[0].WriteChan
		for input := range task.Inputs[0].ReadChan {
			println("filter run 2", input.String())
			outs := fn.Call([]reflect.Value{input})
			if outs[0].Bool() {
				outChan.Send(input)
			}
		}
	}

	d.context.Tasks = append(d.context.Tasks, task)
	return
}

func (t *Task) Run() {
	t.Function()
}

func (d *Dataset) RunSelf() {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var t reflect.Value
		for ok := true; ok; {
			if t, ok = d.WriteChan.Recv(); ok {
				// fmt.Printf("%s -> r\n", t)
				d.ReadChan <- t
			}
		}
		println("closing generator")
		close(d.ReadChan)
	}()
	if d.Generator != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.Generator()
			println("end of the file!")
		}()
	}
	wg.Wait()
	return
}

func NewFlowContext() *FlowContext {
	return &FlowContext{}
}

func main() {
	flag.Parse()

	TextFile("/etc/passwd").Map(func(line string, ch chan string) {
		ch <- line
	}).Filter(func(line string) bool {
		return !strings.HasPrefix(line, "#")
	}).Map(func(line string, ch chan string) {
		println(line)
	}).Run()

}

func TextFile(fname string) (ret *Dataset) {
	ret = NewDataset(NewFlowContext(), reflect.TypeOf(""))
	ret.Generator = func() {
		// println("generate", fname)
		file, err := os.Open(fname)
		if err != nil {
			ret.ErrorChan <- err
			return
		}
		defer file.Close()

		defer ret.WriteChan.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			// println("w <-", scanner.Text())
			ret.WriteChan.Send(reflect.ValueOf(scanner.Text()))
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

	// collect all dataset nodes
	initialDatasets := make(map[*Dataset]bool)
	for _, t := range d.context.Tasks {
		for _, d := range t.Inputs {
			initialDatasets[d] = true
		}
		for _, d := range t.Outputs {
			initialDatasets[d] = true
		}
	}

	// start all dataset nodes
	for k, _ := range initialDatasets {
		wg.Add(1)
		go func(k *Dataset) {
			defer wg.Done()
			k.RunSelf()
			println("init dataset stopped!")
		}(k)
	}

	// start all task edges
	for _, t := range d.context.Tasks {
		wg.Add(1)
		go func(t *Task) {
			defer wg.Done()
			t.Run()
		}(t)
	}
	wg.Wait()
}
