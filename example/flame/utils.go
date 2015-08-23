package flame

import (
	"reflect"
)

func guessFunctionOutputType(f interface{}) reflect.Type {
	ft := reflect.TypeOf(f)
	if ft.In(ft.NumIn()-1).Kind() == reflect.Chan {
		return ft.In(ft.NumIn() - 1).Elem()
	}
	if ft.NumOut() == 1 {
		return ft.Out(0)
	}
	return nil
}

func guessKey(input reflect.Value) reflect.Value {
	switch input.Kind() {
	case reflect.Struct:
		return input.Field(0)
	case reflect.Array, reflect.Slice:
		return input.Index(0)
	}
	return input
}
