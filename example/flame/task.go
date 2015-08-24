package flame

import (
	"reflect"
	"sync"
)

type Task struct {
	Id      int
	Inputs  []*DatasetShard
	Outputs []*DatasetShard
	Step    *Step
}

type StepType int

const (
	OneToOne StepType = iota
	OneToAll
	AllToOne
	AllToAll
)

type Step struct {
	Id       int
	Input    *Dataset
	Output   *Dataset
	Function func(*Task)
	Tasks    []*Task
	Type     StepType
}

func (s *Step) Run() {
	var wg sync.WaitGroup
	for i, t := range s.Tasks {
		wg.Add(1)
		go func(i int, t *Task) {
			defer wg.Done()
			t.Run()
		}(i, t)
	}
	wg.Wait()

	// need to globally close output channels
	switch s.Type {
	case AllToAll:
		for _, shard := range s.Tasks[0].Outputs {
			shard.WriteChan.Close()
		}
	}
	return
}

// source ->w:ds:r -> task -> w:ds:r
// source close next ds' w chan
// ds close its own r chan
// task closes its own channel to next ds' w:ds

func (t *Task) Run() {
	// println("run  step", t.Step.Id, "task", t.Id)
	t.Step.Function(t)
	// println("stop step", t.Step.Id, "task", t.Id)
}

func (t *Task) InputChan() chan reflect.Value {
	if len(t.Inputs) == 1 {
		return t.Inputs[0].ReadChan
	}
	var prevChans []chan reflect.Value
	for _, c := range t.Inputs {
		prevChans = append(prevChans, c.ReadChan)
	}
	println("merge input chan:", len(prevChans))
	return merge(prevChans)
}

func merge(cs []chan reflect.Value) (out chan reflect.Value) {
	var wg sync.WaitGroup

	out = make(chan reflect.Value)

	counter, total := 0, len(cs)

	for _, c := range cs {
		wg.Add(1)
		go func(c chan reflect.Value) {
			defer wg.Done()
			for n := range c {
				out <- n
			}
			counter++
			if total > 1 {
				println("Closed", counter, "/", total)
			}
		}(c)
	}

	go func() {
		wg.Wait()
		if total > 1 {
			println("Finally close chan, ", counter, "/", total)
		}
		close(out)
	}()
	return
}
