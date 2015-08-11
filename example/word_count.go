package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sync"
)

var (
	name = flag.String("name", "worker", "a service name")
)

// T is an alias for interface{} to make code shorter.
type T interface{}

type Task struct {
	Inputs   []*Dataset
	Outputs  []*Dataset
	Function interface{}
}

func NewTask(inputs []*Dataset, outputs []*Dataset, function interface{}) *Task {
	return &Task{Inputs: inputs, Outputs: outputs, Function: function}
}

func (t *Task) Run() {
	// function
	// ft := reflect.TypeOf(t.Function)
	fn := reflect.ValueOf(t.Function)
	// input, type is same as parent Dataset's type
	// output chan, element type is same as current Dataset's type
	ch := t.Outputs[0].WriteChan
	// println("task run 1")
	for input := range t.Inputs[0].ReadChan {
		// println("task run 2", input.String())
		fn.Call([]reflect.Value{input, ch})
	}
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

func (d *Dataset) Map(f interface{}) (ret *Dataset) {
	t := reflect.TypeOf(f)
	if d.Type != t.In(0) {
		panic(fmt.Sprintf("The input %v does not match function input %v", d.Type, t.In(0)))
	}

	ret = NewDataset(d.context, t.In(1).Elem())
	ret.DependsOn(d)

	d.context.Tasks = append(d.context.Tasks, NewTask([]*Dataset{d}, []*Dataset{ret}, f))
	return
}

func (d *Dataset) RunSelf() {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ok := true; ok; {
			if t, ok := d.WriteChan.Recv(); ok {
				// fmt.Printf("%s -> r\n", t)
				d.ReadChan <- t
			}
		}
	}()
	if d.Generator != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.Generator()
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

	NewFlowContext().TextFile("/etc/passwd").Map(func(line string, ch chan string) {
		println(line)
	}).Run()
}

func (fc *FlowContext) TextFile(fname string) (ret *Dataset) {
	ret = NewDataset(fc, reflect.TypeOf(""))
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
