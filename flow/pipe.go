// flow
package flow

import ()

type Task struct {
	function interface{}
	Parent   *Task
}

type FlowContext struct {
	SourceTasks []Task
}

type Flow struct {
	Context FlowContext
}

type Mapper func([]byte) []byte

func (f *Flow) Map(mapper Mapper) {
	f.Context.functions = append(f.Context.functions, mapper)
}
