package flame

import (
	"sync"
)

type Task struct {
	Id      int
	Inputs  []*DatasetShard
	Outputs []*DatasetShard
	Step    *Step
}
type Step struct {
	Id       int
	Input    AbstractDataset
	Output   AbstractDataset
	Function func(*Task)
	Tasks    []*Task
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
