package flame

import (
	"sync"
)

var runners []Runner

// Invoked by driver task runner
func RegisterRunner(r Runner) {
	runners = append(runners, r)
}

type Runner interface {
	Run(fc *FlowContext)
	ShouldRun() bool
}

func (fc *FlowContext) Run() {
	// hook to run task runners
	for _, r := range runners {
		if r.ShouldRun() {
			r.Run(fc)
			return
		}
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
