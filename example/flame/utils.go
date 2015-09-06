package flame

import (
	"reflect"
)

type KeyValues struct {
	Key    interface{}
	Values []interface{}
}

type KeyValue struct {
	Key   interface{}
	Value interface{}
}

func guessFunctionOutputType(f interface{}) reflect.Type {
	ft := reflect.TypeOf(f)
	if ft.In(ft.NumIn()-1).Kind() == reflect.Chan {
		return ft.In(ft.NumIn() - 1).Elem()
	}
	if ft.NumOut() == 1 {
		return ft.Out(0)
	}
	if ft.NumOut() == 2 {
		return reflect.TypeOf(KeyValue{})
	}
	return nil
}

var KeyValueType reflect.Type

func init() {
	KeyValueType = reflect.TypeOf(KeyValue{})
}

func guessKey(input reflect.Value) reflect.Value {
	switch input.Kind() {
	case reflect.Struct:
		if input.Type() == KeyValueType {
			key := input.Field(0)
			return reflect.ValueOf(key.Interface())
		}
		key := input.Field(0)
		if v, ok := key.Interface().(reflect.Value); ok {
			return v
		}
		return key
	case reflect.Array, reflect.Slice:
		return input.Index(0)
	}
	return input
}
