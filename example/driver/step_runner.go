package driver

import (
	"sync"

	"github.com/chrislusf/netchan/example/flame"
)

type StepRunner struct {
	Step *flame.Step
}

func NewStepRunner(s *flame.Step) *StepRunner {
	return &StepRunner{Step: s}
}

func (sr *StepRunner) Run() {
	s := sr.Step
	var wg sync.WaitGroup
	for _, t := range s.Tasks {
		wg.Add(1)
		go func(t *flame.Task) {
			defer wg.Done()

			// 1. find a machine for task t
			// 2. invoke t
			t.Run()
		}(t)
	}
	wg.Wait()

	switch s.Type {
	case flame.OneToOne:
		for _, t := range s.Tasks {
			for _, out := range t.Outputs {
				out.WriteChan.Close()
			}
		}
	case flame.OneToAll, flame.AllToOne, flame.AllToAll:
		for _, shard := range s.Tasks[0].Outputs {
			shard.WriteChan.Close()
		}
	}
	// close the output channel

	return
}
