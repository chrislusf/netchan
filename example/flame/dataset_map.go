package flame

import (
	"reflect"
)

// map can work with multiple kinds of inputs and outputs
// 1. If 2 inputs, the first input is key, the second input is value
// 2. If 1 input, the input is value.
// 3. If last input is channel, the output goes into the channel
// 4. If 2 output, the first output is key, the second output is value
// 5. If 1 output, the output is value.
// 6. A map function may not necessarily have any output.
//
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
		} else if ft.NumOut() == 2 {
			outChan := task.Outputs[0].WriteChan
			if d.Type.Kind() == reflect.Struct && ft.NumIn() != 1 {
				invokeMapFunc = func(input reflect.Value) {
					var args []reflect.Value
					for i := 0; i < input.NumField(); i++ {
						args = append(args, input.Field(i))
					}
					outs := fn.Call(args)
					// outChan.Send(reflect.ValueOf(KeyValue{Key: outs[0].Interface(), Value: outs[1].Interface()}))
					sendValues(outChan, outs)
				}
			} else {
				invokeMapFunc = func(input reflect.Value) {
					outs := fn.Call([]reflect.Value{input})
					// outChan.Send(reflect.ValueOf(KeyValue{Key: outs[0].Interface(), Value: outs[1].Interface()}))
					sendValues(outChan, outs)
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
			} else if d.Type.Kind() == reflect.Slice && ft.NumIn() != 1 {
				invokeMapFunc = func(input reflect.Value) {
					var args []reflect.Value
					for i := 0; i < input.Len(); i++ {
						args = append(args, reflect.ValueOf(input.Index(i).Interface()))
					}
					fn.Call(args)
				}
			} else {
				// println("d.Type.Kind()", d.Type.Kind().String())
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
		d.context.Run()
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

func sendValues(outChan reflect.Value, values []reflect.Value) {
	var infs []interface{}
	for _, v := range values {
		infs = append(infs, v.Interface())
	}
	outChan.Send(reflect.ValueOf(infs))
}
