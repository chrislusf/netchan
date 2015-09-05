package flame

import (
	"reflect"
)

type KeyValues struct {
	Key    reflect.Value
	Values []reflect.Value
}

type KeyValue struct {
	Key   reflect.Value
	Value reflect.Value
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

func guessKey(input reflect.Value) reflect.Value {
	switch input.Kind() {
	case reflect.Struct:
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
