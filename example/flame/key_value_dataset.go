package flame

import (
	"reflect"

	"github.com/psilva261/timsort"
)

// f(V, V) bool : less than function
// New Dataset contains K,V
func (d *Dataset) LocalSort(f interface{}) (ret *Dataset) {
	ret = d.newNextDataset(len(d.Shards), d.Type)
	step := d.context.AddStep(d, ret)
	step.Function = func(task *Task) {
		lessThanFuncValue := reflect.ValueOf(f)
		outChan := task.Outputs[0].WriteChan
		var kvs []interface{}
		for input := range task.Inputs[0].ReadChan {
			kvs = append(kvs, input.Interface())
		}
		timsort.Sort(kvs, func(a interface{}, b interface{}) bool {
			ret := lessThanFuncValue.Call([]reflect.Value{
				reflect.ValueOf(a).Field(0),
				reflect.ValueOf(b).Field(0),
			})
			return ret[0].Bool()
		})
		for _, kv := range kvs {
			outChan.Send(reflect.ValueOf(kv))
		}
		outChan.Close()
	}
	return ret
}
