package flame

import (
	"bufio"
	"os"
	"reflect"
	"sync"
)

func (d *Dataset) GetShards() []*DatasetShard {
	return d.Shards
}

type Dataset struct {
	context *FlowContext
	Type    reflect.Type
	Shards  []*DatasetShard

	ErrorChan chan error
	Generator func()
}

type DatasetShard struct {
	Parent    *Dataset
	ReadChan  chan reflect.Value
	WriteChan reflect.Value
}

func NewDataset(context *FlowContext, t reflect.Type) *Dataset {
	return &Dataset{
		context:   context,
		Type:      t,
		ErrorChan: make(chan error, 0),
	}
}

// f(A, chan B)
// input, type is same as parent Dataset's type
// output chan, element type is same as current Dataset's type
func (d *Dataset) Map(f interface{}) (ret *Dataset) {
	ret = d.newNextDataset(len(d.Shards), guessMapFuncReturnType(f))
	step := d.context.AddOneToOneStep(d, ret)
	step.Function = func(task *Task) {
		fn := reflect.ValueOf(f)
		outChan := task.Outputs[0].WriteChan
		defer outChan.Close()

		ft := reflect.TypeOf(f)
		if ft.In(ft.NumIn()-1).Kind() == reflect.Chan {
			for input := range task.InputChan() {
				// println("func:", ft.String(), "input:", input.Type().String(), "outChan:", outChan.Type().String())
				if input.Kind() == reflect.Struct && ft.NumIn() != 2 {
					var args []reflect.Value
					for i := 0; i < input.NumField(); i++ {
						args = append(args, input.Field(i))
					}
					args = append(args, outChan)
					fn.Call(args)
				} else {
					fn.Call([]reflect.Value{input, outChan})
				}
			}
		} else if ft.NumOut() == 1 {
			for input := range task.InputChan() {
				var outs []reflect.Value
				if input.Kind() == reflect.Struct && ft.NumIn() != 1 {
					var args []reflect.Value
					for i := 0; i < input.NumField(); i++ {
						args = append(args, input.Field(i))
					}
					outs = fn.Call(args)
				} else {
					outs = fn.Call([]reflect.Value{input})
				}
				outChan.Send(outs[0])
			}
		}

	}
	return
}

func guessMapFuncReturnType(f interface{}) reflect.Type {
	ft := reflect.TypeOf(f)
	if ft.In(ft.NumIn()-1).Kind() == reflect.Chan {
		return ft.In(ft.NumIn() - 1).Elem()
	}
	if ft.NumOut() == 1 {
		return ft.Out(0)
	}
	panic("Not sure the output type in " + ft.String())
}

// f(A)bool
func (d *Dataset) Filter(f interface{}) (ret *Dataset) {
	ret = d.newNextDataset(len(d.Shards), d.Type)
	step := d.context.AddOneToOneStep(d, ret)
	step.Function = func(task *Task) {
		fn := reflect.ValueOf(f)
		outChan := task.Outputs[0].WriteChan
		defer outChan.Close()
		for input := range task.InputChan() {
			outs := fn.Call([]reflect.Value{input})
			if outs[0].Bool() {
				outChan.Send(input)
			}
		}
	}
	return
}

func TextFile(fname string, shard int) (ret *Dataset) {
	ret = NewDataset(&FlowContext{}, reflect.TypeOf(""))
	ret.Shard(shard)
	ret.Generator = func() {
		// println("generate", fname)
		file, err := os.Open(fname)
		if err != nil {
			ret.ErrorChan <- err
			return
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		var i int
		for scanner.Scan() {
			ret.Shards[i].WriteChan.Send(reflect.ValueOf(scanner.Text()))
			i++
			if i == shard {
				i = 0
			}
		}

		for _, s := range ret.Shards {
			// println("closing source shard", i, "w")
			s.WriteChan.Close()
		}

		if err := scanner.Err(); err != nil {
			ret.ErrorChan <- err
			return
		}
	}
	return
}

func (d *Dataset) Run() {
	var wg sync.WaitGroup

	// start all task edges
	for i, step := range d.context.Steps {
		if i == 0 {
			wg.Add(1)
			go func(step *Step) {
				defer wg.Done()
				// println("start dataset", step.Id)
				step.Input.RunSelf(step.Id)
			}(step)
		}
		wg.Add(1)
		go func(step *Step) {
			defer wg.Done()
			step.Run()
		}(step)
		wg.Add(1)
		go func(step *Step) {
			defer wg.Done()
			// println("start dataset", step.Id+1)
			step.Output.RunSelf(step.Id + 1)
		}(step)
	}
	wg.Wait()
}

func (d *Dataset) RunSelf(stepId int) {
	var wg sync.WaitGroup
	for shardId, shard := range d.Shards {
		wg.Add(1)
		go func(shardId int, shard *DatasetShard) {
			defer wg.Done()
			var t reflect.Value
			for ok := true; ok; {
				if t, ok = shard.WriteChan.Recv(); ok {
					// fmt.Printf("%s -> r\n", t)
					shard.ReadChan <- t
				}
			}
			// println("dataset", stepId, "shard", shardId, "close r")
			close(shard.ReadChan)
		}(shardId, shard)
	}
	if d.Generator != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.Generator()
		}()
	}
	wg.Wait()
	// println("dataset", stepId, "stopped")
	return
}
