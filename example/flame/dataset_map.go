package flame

import (
	"reflect"
)

// f(A, chan B)
// input, type is same as parent Dataset's type
// output chan, element type is same as current Dataset's type
func (d *Dataset) Map(f interface{}) (ret *Dataset) {
	outType := guessFunctionOutputType(f)
	ret = d.context.newNextDataset(len(d.Shards), outType)
	step := d.context.AddOneToOneStep(d, ret)
	step.Function = func(task *Task) {
		fn := reflect.ValueOf(f)
		ft := reflect.TypeOf(f)

		var invokeMapFunc func(input reflect.Value)

		if ft.In(ft.NumIn()-1).Kind() == reflect.Chan {
			outChan := task.Outputs[0].WriteChan
			if d.Type.Kind() == reflect.Struct && ft.NumIn() != 2 {
				invokeMapFunc = func(input reflect.Value) {
					var args []reflect.Value
					for i := 0; i < input.NumField(); i++ {
						args = append(args, input.Field(i))
					}
					args = append(args, outChan)
					fn.Call(args)
				}
			} else {
				invokeMapFunc = func(input reflect.Value) {
					fn.Call([]reflect.Value{input, outChan})
				}
			}
		} else if ft.NumOut() == 1 {
			outChan := task.Outputs[0].WriteChan
			if d.Type.Kind() == reflect.Struct && ft.NumIn() != 1 {
				invokeMapFunc = func(input reflect.Value) {
					var args []reflect.Value
					for i := 0; i < input.NumField(); i++ {
						args = append(args, input.Field(i))
					}
					outs := fn.Call(args)
					outChan.Send(outs[0])
				}
			} else {
				invokeMapFunc = func(input reflect.Value) {
					outs := fn.Call([]reflect.Value{input})
					outChan.Send(outs[0])
				}
			}
		} else {
			if d.Type.Kind() == reflect.Struct && ft.NumIn() != 1 {
				invokeMapFunc = func(input reflect.Value) {
					var args []reflect.Value
					for i := 0; i < input.NumField(); i++ {
						args = append(args, input.Field(i))
					}
					fn.Call(args)
				}
			} else {
				invokeMapFunc = func(input reflect.Value) {
					fn.Call([]reflect.Value{input})
				}
			}
		}

		for input := range task.InputChan() {
			invokeMapFunc(input)
		}
	}
	if ret == nil {
		d.Run()
	}
	return
}

// f(A)bool
func (d *Dataset) Filter(f interface{}) (ret *Dataset) {
	ret = d.context.newNextDataset(len(d.Shards), d.Type)
	step := d.context.AddOneToOneStep(d, ret)
	step.Function = func(task *Task) {
		fn := reflect.ValueOf(f)
		outChan := task.Outputs[0].WriteChan
		for input := range task.InputChan() {
			outs := fn.Call([]reflect.Value{input})
			if outs[0].Bool() {
				outChan.Send(input)
			}
		}
	}
	return
}
