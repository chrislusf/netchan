package flame

import (
	"log"
	"reflect"

	"github.com/chrislusf/netchan/example/lib"
	"github.com/psilva261/timsort"
)

func DefaultStringLessThanComparator(a, b string) bool {
	return a < b
}
func DefaultInt64LessThanComparator(a, b int64) bool {
	return a < b
}
func DefaultFloat64LessThanComparator(a, b float64) bool {
	return a < b
}

func getLessThanComparator(dt reflect.Type) (funcPointer interface{}) {
	switch dt.Kind() {
	case reflect.Int:
		funcPointer = DefaultInt64LessThanComparator
	case reflect.Float64:
		funcPointer = DefaultFloat64LessThanComparator
	case reflect.String:
		funcPointer = DefaultStringLessThanComparator
	default:
		log.Panicf("No default less than comparator for %s:%s", dt.String(), dt.Kind().String())
	}
	return
}

func (d *Dataset) Sort(f interface{}) (ret *Dataset) {
	return d.LocalSort(f).MergeSorted(f)
}

// f(V, V) bool : less than function
// New Dataset contains K,V
func (d *Dataset) LocalSort(f interface{}) (ret *Dataset) {
	ret = d.context.newNextDataset(len(d.Shards), d.Type)
	step := d.context.AddOneToOneStep(d, ret)
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan
		defer outChan.Close()
		var kvs []interface{}
		for input := range task.InputChan() {
			kvs = append(kvs, input.Interface())
		}
		lessThanFuncValue := reflect.ValueOf(f)
		if f == nil && len(kvs) > 0 {
			dt := reflect.TypeOf(kvs[0])
			if kv, isKeyValue := kvs[0].(KeyValue); isKeyValue {
				dt = kv.Key.Type()
			}
			f = getLessThanComparator(dt)
			lessThanFuncValue = reflect.ValueOf(f)
		}
		// println("set lessThanFuncValue", lessThanFuncValue.String(), "len(kvs)", len(kvs))
		// println("got all inputs")
		// if this is stuck, usually means upstream some chan is not closed
		// since the source waits for all inputs

		if d.Type.Kind() == reflect.Struct {
			timsort.Sort(kvs, func(a interface{}, b interface{}) bool {
				// println("a:", reflect.ValueOf(a).Field(0).Interface().(reflect.Value).Kind().String(), "lessThanFuncValue:", lessThanFuncValue.String())
				ret := lessThanFuncValue.Call([]reflect.Value{
					reflect.ValueOf(a).Field(0).Interface().(reflect.Value),
					reflect.ValueOf(b).Field(0).Interface().(reflect.Value),
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
	step := d.context.AddAllToOneStep(d, ret)
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
