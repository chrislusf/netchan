package flame

import (
	"reflect"
)

func (d *Dataset) Reduce(f interface{}) (ret *Dataset) {
	return d.LocalReduce(f).MergeReduce(f)
}

// f(V, V) V : less than function
// New Dataset contains V
func (d *Dataset) LocalReduce(f interface{}) (ret *Dataset) {
	ret = d.newNextDataset(len(d.Shards), d.Type)
	step := d.context.AddOneToOneStep(d, ret)
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan
		isFirst := true
		var localResult reflect.Value
		fn := reflect.ValueOf(f)
		for input := range task.InputChan() {
			if isFirst {
				isFirst = false
				localResult = input
			} else {
				outs := fn.Call([]reflect.Value{
					localResult,
					input,
				})
				localResult = outs[0]
			}
		}
		outChan.Send(localResult)
		outChan.Close()
	}
	return ret
}

func (d *Dataset) MergeReduce(f interface{}) (ret *Dataset) {
	ret = d.newNextDataset(1, d.Type)
	step := d.context.AddManyToOneStep(d, ret)
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan
		isFirst := true
		var localResult reflect.Value
		fn := reflect.ValueOf(f)
		for input := range task.InputChan() {
			if isFirst {
				isFirst = false
				localResult = input
			} else {
				outs := fn.Call([]reflect.Value{
					localResult,
					input,
				})
				localResult = outs[0]
			}
		}
		outChan.Send(localResult)
		outChan.Close()
	}
	return ret
}
