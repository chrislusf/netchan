package flame

import (
	"fmt"
	"reflect"

	"github.com/psilva261/timsort"
)

type KeyValueDataset struct {
	Dataset
	KeyType reflect.Type
}

func NewKeyValueDataset(context *FlowContext, keyType, valueType reflect.Type) *KeyValueDataset {
	base := NewDataset(context, valueType)
	return CastToKeyValueDataset(base, keyType)
}

func CastToKeyValueDataset(base *Dataset, keyType reflect.Type) *KeyValueDataset {
	return &KeyValueDataset{*base, keyType}
}

// f(A, chan KeyValue{K,V})
func (d *Dataset) MapToKeyValueDataset(f interface{}) *KeyValueDataset {
	ft := reflect.TypeOf(f)
	fn := reflect.ValueOf(f)
	elem := ft.In(1).Elem()
	newDataset := NewKeyValueDataset(d.context, elem.Field(0).Type, elem.Field(1).Type)

	ctype := reflect.ChanOf(reflect.BothDir, elem)
	for i := 0; i < len(d.Shards); i++ {
		ds := &DatasetShard{
			Parent:    d,
			ReadChan:  make(chan reflect.Value, 0),
			WriteChan: reflect.MakeChan(ctype, 0),
		}
		newDataset.Shards = append(newDataset.Shards, ds)
	}
	newDataset.Type = elem
	// println("dataset stored type", elem.String())

	step := d.context.AddStep(d, newDataset)
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan
		for input := range task.Inputs[0].ReadChan {
			fn.Call([]reflect.Value{input, outChan})
		}
		outChan.Close()
	}

	return newDataset
}

func (d *KeyValueDataset) Reduce(shardFunc, sortFunc, mergeFunc interface{}) *KeyValueDataset {
	return d.ShardByKey(shardFunc).LocalSort(sortFunc).LocalReduce(mergeFunc)
}

// f(K) int
func (d *KeyValueDataset) ShardByKey(f interface{}) *KeyValueDataset {
	d.Dataset.assertType(f)
	ft := reflect.TypeOf(f)
	ret := NewKeyValueDataset(d.context, d.KeyType, ft.Out(0))
	ret.Shard(len(d.Shards), d.ValueType)

	step := d.context.AddStep(&d.Dataset, &ret.Dataset)
	reduceExecution := reflect.ValueOf(f)
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan
		var prevKey reflect.Value
		reduced := reflect.New(ft.Out(0))
		for input := range task.Inputs[0].ReadChan {
			key, value := input.Field(0), input.Field(1)
			if reflect.DeepEqual(key, prevKey) {
				outs := reduceExecution.Call([]reflect.Value{value, reduced})
				reduced = outs[0]
			} else {
				prevKey = key
				reduced = reflect.New(ft.Out(0))
			}

		}
		outChan.Close()
	}

	return ret
}

// f(V, R) R, both commutative and associative.
// New KeyValueDataset contains K,R
func (d *KeyValueDataset) LocalReduce(f interface{}) *KeyValueDataset {
	ft := reflect.TypeOf(f)
	if d.ValueType != ft.In(0) {
		panic(fmt.Sprintf("The input %v does not match function value input %v", d.ValueType, ft.In(0)))
	}

	ret := NewKeyValueDataset(d.context, d.KeyType, ft.Out(0))
	ret.Shard(len(d.Shards), d.ValueType)

	step := d.context.AddStep(&d.Dataset, &ret.Dataset)
	reduceExecution := reflect.ValueOf(f)
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan
		var prevKey reflect.Value
		reduced := reflect.New(ft.Out(0))
		for input := range task.Inputs[0].ReadChan {
			key, value := input.Field(0), input.Field(1)
			if reflect.DeepEqual(key, prevKey) {
				outs := reduceExecution.Call([]reflect.Value{value, reduced})
				reduced = outs[0]
			} else {
				prevKey = key
				reduced = reflect.New(ft.Out(0))
			}

		}
		outChan.Close()
	}
	return ret
}

// f(V, V) bool : less than function
// New KeyValueDataset contains K,V
func (d *KeyValueDataset) LocalSort(f interface{}) *KeyValueDataset {
	d.Dataset.assertType(f)

	ret := NewKeyValueDataset(d.context, d.KeyType, d.ValueType)
	ret.Shard(len(d.Shards), d.Type)

	// println("sorted dataset stored type", ret.Type.String(), d.Type.String())

	step := d.context.AddStep(d, ret)
	lessThanFuncValue := reflect.ValueOf(f)
	step.Function = func(task *Task) {
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

// f(K, V, chan KeyValue)
// input, type is same as parent Dataset's type
// output chan, element type is same as current Dataset's type
func (d *KeyValueDataset) Map(f interface{}) *KeyValueDataset {
	d.assertType(f)
	fn := reflect.ValueOf(f)
	ft := reflect.TypeOf(f)
	elem := ft.In(2).Elem()

	ret := NewKeyValueDataset(d.context, elem.Field(0).Type, elem.Field(1).Type)
	ret.Shard(len(d.Shards), elem)

	d.addOneToOneStep(ret, fn, func(key, value reflect.Value, outChan reflect.Value) {
		fn.Call([]reflect.Value{key, value, outChan})
	})
	return ret
}

func (d *KeyValueDataset) assertType(f interface{}) {
	ft := reflect.TypeOf(f)
	if d.KeyType != ft.In(0) {
		panic(fmt.Sprintf("The input %v does not match function input %v", d.KeyType, ft.In(0)))
	}
	if d.ValueType != ft.In(1) {
		panic(fmt.Sprintf("The input %v does not match function input %v", d.ValueType, ft.In(1)))
	}
}

func (d *KeyValueDataset) addOneToOneStep(toKeyValueDataset *KeyValueDataset,
	taskFuncValue reflect.Value,
	taskExecution func(key, value reflect.Value, outChan reflect.Value),
) {
	step := d.context.AddStep(&d.Dataset, &toKeyValueDataset.Dataset)
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan
		for input := range task.Inputs[0].ReadChan {
			key, value := input.Field(0), input.Field(1)
			taskExecution(key, value, outChan)
		}
		outChan.Close()
	}
}
