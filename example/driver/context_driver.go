package driver

import (
	"flag"
	"os"
	"strconv"
	"sync"

	"github.com/chrislusf/netchan/example/driver/cmd"
	"github.com/chrislusf/netchan/example/driver/scheduler"
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

// driver runs on local, controlling all tasks
func (fcd *FlowContextDriver) Run(fc *flame.FlowContext) {

	taskGroups := scheduler.GroupTasks(fc)

	// schedule to run the steps
	var wg sync.WaitGroup
	for _, tg := range taskGroups {
		wg.Add(1)
		go func(taskGroupId int) {
			defer wg.Done()
			dir, _ := os.Getwd()
			request := NewStartRequest(os.Args[0], dir,
				"-task.context.id",
				strconv.Itoa(fc.Id),
				"-task.taskGroup.id",
				strconv.Itoa(taskGroupId),
			)
			if err := RemoteExecute(fcd.option.Leader, "a1", request); err != nil {
				println("exeuction error:", err.Error())
			}
		}(tg.Id)
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
