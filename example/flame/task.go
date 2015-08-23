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
	for _, t := range s.Tasks {
		wg.Add(1)
		go func(t *Task) {
			defer wg.Done()
			t.Run()
		}(t)
	}
	wg.Wait()

	switch s.Type {
	case OneToOne:
		for _, t := range s.Tasks {
			for _, out := range t.Outputs {
				out.WriteChan.Close()
			}
		}
	case OneToAll, AllToOne, AllToAll:
		for _, shard := range s.Tasks[0].Outputs {
			shard.WriteChan.Close()
		}
	}
	// close the output channel

	return
}

// source ->w:ds:r -> task -> w:ds:r
// source close next ds' w chan
// ds close its own r chan
// step, not task, closes next ds' w:ds

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
	return merge(prevChans)
}

func merge(cs []chan reflect.Value) (out chan reflect.Value) {
	var wg sync.WaitGroup

	out = make(chan reflect.Value)

	for _, c := range cs {
		wg.Add(1)
		go func(c chan reflect.Value) {
			defer wg.Done()
			for n := range c {
				out <- n
			}
		}(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return
}
