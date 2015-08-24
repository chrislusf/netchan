package flame

import (
	"reflect"

	"github.com/chrislusf/netchan/example/lib"
	"github.com/psilva261/timsort"
)

func (d *Dataset) Sort(f interface{}) (ret *Dataset) {
	return d.LocalSort(f).MergeSorted(f)
}

// f(V, V) bool : less than function
// New Dataset contains K,V
func (d *Dataset) LocalSort(f interface{}) (ret *Dataset) {
	ret = d.context.newNextDataset(len(d.Shards), d.Type)
	step := d.context.AddOneToOneStep(d, ret)
	step.Function = func(task *Task) {
		lessThanFuncValue := reflect.ValueOf(f)
		outChan := task.Outputs[0].WriteChan
		defer outChan.Close()
		var kvs []interface{}
		for input := range task.InputChan() {
			kvs = append(kvs, input.Interface())
		}
		// println("got all inputs")
		// if this is stuck, usually means upstream some chan is not closed
		// since the source waits for all inputs

		if d.Type.Kind() == reflect.Struct {
			timsort.Sort(kvs, func(a interface{}, b interface{}) bool {
				ret := lessThanFuncValue.Call([]reflect.Value{
					reflect.ValueOf(a).Field(0),
					reflect.ValueOf(b).Field(0),
				})
				return ret[0].Bool()
			})
		} else {
			timsort.Sort(kvs, func(a interface{}, b interface{}) bool {
				ret := lessThanFuncValue.Call([]reflect.Value{
					reflect.ValueOf(a),
					reflect.ValueOf(b),
				})
				return ret[0].Bool()
			})
		}

		for _, kv := range kvs {
			outChan.Send(reflect.ValueOf(kv))
		}

	}
	return ret
}

func (d *Dataset) MergeSorted(f interface{}) (ret *Dataset) {
	ret = d.context.newNextDataset(1, d.Type)
	step := d.context.AddManyToOneStep(d, ret)
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan
		defer outChan.Close()
		fn := reflect.ValueOf(f)
		comparator := func(a, b reflect.Value) bool {
			outs := fn.Call([]reflect.Value{
				a,
				b,
			})
			return outs[0].Bool()
		}
		if d.Type.Kind() == reflect.Struct {
			comparator = func(a, b reflect.Value) bool {
				outs := fn.Call([]reflect.Value{
					a.Field(0),
					b.Field(0),
				})
				return outs[0].Bool()
			}
		}

		pq := lib.NewPriorityQueue(comparator)
		// enqueue one item to the pq from each shard
		for shardId, shard := range task.Inputs {
			if x, ok := <-shard.ReadChan; ok {
				pq.Enqueue(x, shardId)
			}
		}
		for pq.Len() > 0 {
			t, shardId := pq.Dequeue()
			outChan.Send(t)
			if x, ok := <-task.Inputs[shardId].ReadChan; ok {
				pq.Enqueue(x, shardId)
			}
		}
	}
	return ret
}
