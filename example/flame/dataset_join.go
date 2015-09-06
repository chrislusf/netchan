package flame

import (
	"log"
	"reflect"
)

func DefaultStringComparator(a, b string) int64 {
	switch {
	case a == b:
		return 0
	case a < b:
		return -1
	default:
		return 1
	}
}
func DefaultInt64Comparator(a, b int64) int64 {
	return a - b
}
func DefaultFloat64Comparator(a, b float64) int64 {
	switch {
	case a == b:
		return 0
	case a < b:
		return -1
	default:
		return 1
	}
}

func getComparator(dt reflect.Type) (funcPointer interface{}) {
	switch dt.Kind() {
	case reflect.Int:
		funcPointer = DefaultInt64Comparator
	case reflect.Float64:
		funcPointer = DefaultFloat64Comparator
	case reflect.String:
		funcPointer = DefaultStringComparator
	default:
		log.Panicf("No default comparator for %s:%s", dt.String(), dt.Kind().String())
	}
	return
}

// assume nothing about these two dataset
func (d *Dataset) Join(other *Dataset) *Dataset {
	d = d.Partition(len(d.Shards)).LocalSort(nil)
	other = other.Partition(len(d.Shards)).LocalSort(nil)
	return d.JoinHashedSorted(other, nil, false, false)
}

// Join multiple datasets that are sharded by the same key, and locally sorted within the shard
// "this" dataset is the driving dataset and should have more data than "that"
func (this *Dataset) JoinHashedSorted(that *Dataset,
	compareFunc interface{}, isLeftOuterJoin, isRightOuterJoin bool,
) (ret *Dataset) {
	outType := reflect.TypeOf(([]interface{})(nil))
	ret = this.context.newNextDataset(len(this.Shards), outType)

	inputs := []*Dataset{this, that}
	step := this.context.MergeOneShardToOneShardStep(inputs, ret)

	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan

		leftChan := task.Inputs[0].ReadChan
		rightChan := task.Inputs[1].ReadChan

		// get first value from both channels
		rightKey, rightValue, rightHasValue := getKeyValue(rightChan)
		leftKey, leftValue, leftHasValue := getKeyValue(leftChan)

		if compareFunc == nil {
			compareFunc = getComparator(leftKey.Type())
		}
		fn := reflect.ValueOf(compareFunc)
		comparator := func(a, b reflect.Value) int64 {
			outs := fn.Call([]reflect.Value{a, b})
			return outs[0].Int()
		}

		for leftHasValue {
			if !rightHasValue {
				if isLeftOuterJoin {
					outChan.Send(reflect.ValueOf([]interface{}{leftKey, leftValue, nil}))
				}
				leftKey, leftValue, leftHasValue = getKeyValue(leftChan)
				continue
			}

			x := comparator(leftKey, rightKey)
			switch {
			case x == 0:
				// collect all values on rightChan
				rightValues := []reflect.Value{rightValue}
				prevRightKey := rightKey
				for {
					rightKey, rightValue, rightHasValue = getKeyValue(rightChan)
					if rightHasValue && comparator(prevRightKey, rightKey) == 0 {
						rightValues = append(rightValues, rightValue)
						continue
					} else {
						// for current left key, join with all rightValues
						for _, rv := range rightValues {
							outChan.Send(reflect.ValueOf([]interface{}{leftKey, leftValue, rv}))
						}
						// reset right loop
						rightValues = rightValues[0:0]
						leftKey, leftValue, leftHasValue = getKeyValue(leftChan)
						break
					}
				}
			case x < 0:
				if isLeftOuterJoin {
					outChan.Send(reflect.ValueOf([]interface{}{leftKey, leftValue, nil}))
				}
				leftKey, leftValue, leftHasValue = getKeyValue(leftChan)
			case x > 0:
				if isRightOuterJoin {
					outChan.Send(reflect.ValueOf([]interface{}{rightKey, nil, rightValue}))
				}
				rightKey, rightValue, rightHasValue = getKeyValue(rightChan)
			}
		}
		// handle right outer join tail case
		if isRightOuterJoin {
			for rightKeyValue := range rightChan {
				rightKey, rightValue := rightKeyValue.Field(0), rightKeyValue.Field(1)
				outChan.Send(reflect.ValueOf([]interface{}{rightKey, nil, rightValue}))
			}
		}

	}
	return ret
}

func getKeyValue(ch chan reflect.Value) (key, value reflect.Value, ok bool) {
	keyValue, hasValue := <-ch
	if hasValue {
		key = reflect.ValueOf(keyValue.Field(0).Interface())
		value = reflect.ValueOf(keyValue.Field(1).Interface())
	}
	return key, value, hasValue
}

func iterateByKey(ch chan reflect.Value, outChan chan KeyValues) {
	var prevKey, key, value interface{}
	var values []interface{}
	isFirst := true
	for kv := range ch {
		key, value = kv.Field(0), kv.Field(1)
		if !isFirst && !reflect.DeepEqual(prevKey, key) {
			outChan <- KeyValues{Key: key, Values: values}
			prevKey = key
			values = values[0:0]
		} else {
			values = append(values, value)
		}
	}
	outChan <- KeyValues{Key: key, Values: values}
}
