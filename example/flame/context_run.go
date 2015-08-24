package flame

import (
	"sync"
)

var runner Runner

// Invoked by driver task runner
func RegisterRunner(r Runner) {
	runner = r
}

type Runner interface {
	Run() bool
}

func (fc *FlowContext) Run() {
	// hook to run task runner
	if runner != nil && runner.Run() {
		return
	}

	var wg sync.WaitGroup

	// start all task edges
	for i, step := range fc.Steps {
		if i == 0 {
			wg.Add(1)
			go func(step *Step) {
				defer wg.Done()
				// println("start dataset", step.Id)
				if step.Input != nil {
					step.Input.RunSelf(step.Id)
				}
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
			if step.Output != nil {
				step.Output.RunSelf(step.Id + 1)
			}
		}(step)
	}
	wg.Wait()
}
