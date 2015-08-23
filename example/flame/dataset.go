package flame

import (
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

func (d *Dataset) Run() {
	var wg sync.WaitGroup

	// start all task edges
	for i, step := range d.context.Steps {
		if i == 0 {
			wg.Add(1)
			go func(step *Step) {
				defer wg.Done()
				// println("start dataset", step.Id)
				if step.Input != nil {
					step.Input.RunSelf(step.Id)
				}
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
			if step.Output != nil {
				step.Output.RunSelf(step.Id + 1)
			}
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
