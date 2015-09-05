package driver

import (
	"flag"
	"os"
	"strconv"
	"sync"

	"github.com/chrislusf/netchan/example/driver/cmd"
	"github.com/chrislusf/netchan/example/flame"
	"github.com/golang/protobuf/proto"
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

	// find all possible resources

	// schedule to run the steps
	var wg sync.WaitGroup
	for stepId, step := range fc.Steps {
		for taskId, task := range step.Tasks {
			wg.Add(1)
			go func(stepId, taskId int, task *flame.Task) {
				defer wg.Done()
				dir, _ := os.Getwd()
				cmd := NewStartRequest(os.Args[0], dir,
					"-task.context.id",
					strconv.Itoa(fc.Id),
					"-task.step.id",
					strconv.Itoa(stepId),
					"-task.task.id",
					strconv.Itoa(taskId),
				)
				if err := RemoteExecute(fcd.option.Leader, "a1", cmd); err != nil {
					println("exeuction error:", err.Error())
				}
			}(stepId, taskId, task)
		}
	}
	wg.Wait()
}

func NewStartRequest(path string, dir string, args ...string) *cmd.ControlMessage {
	return &cmd.ControlMessage{
		Type: cmd.ControlMessage_StartRequest.Enum(),
		StartRequest: &cmd.StartRequest{
			Path: proto.String(path),
			Args: args,
			Dir:  proto.String(dir),
		},
	}
}
