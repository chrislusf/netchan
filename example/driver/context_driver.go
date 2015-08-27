package driver

import (
	"flag"
	"os"
	"os/exec"
	"strconv"
	"sync"

	"github.com/chrislusf/netchan/example/flame"
)

type DriverOption struct {
	ShouldStart bool
	Leader      string
}

func init() {
	var driverOption DriverOption
	flag.BoolVar(&driverOption.ShouldStart, "driver", false, "start in driver mode")
	flag.StringVar(&driverOption.Leader, "driver.leader", "localhost:8930", "leader server")

	flame.RegisterContextRunner(NewFlowContextDriver(&driverOption))
}

type FlowContextDriver struct {
	option *DriverOption
}

func NewFlowContextDriver(option *DriverOption) *FlowContextDriver {
	return &FlowContextDriver{option: option}
}

func (fcd *FlowContextDriver) IsDriverMode() bool {
	return fcd.option.ShouldStart
}

func (fcd *FlowContextDriver) ShouldRun(fc *flame.FlowContext) bool {
	return fcd.option.ShouldStart
}

// driver runs on local, controlling all tasks
func (fcd *FlowContextDriver) Run(fc *flame.FlowContext) {

	var wg sync.WaitGroup

	// start all tasks
	for stepId, step := range fc.Steps {
		for taskId, task := range step.Tasks {
			wg.Add(1)
			go func(stepId, taskId int, task *flame.Task) {
				defer wg.Done()
				cmd := exec.Command(os.Args[0],
					"-task.context.id",
					strconv.Itoa(fc.Id),
					"-task.step.id",
					strconv.Itoa(stepId),
					"-task.task.id",
					strconv.Itoa(taskId),
				)
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				cmd.Run()
			}(stepId, taskId, task)
		}
	}
	wg.Wait()
}
