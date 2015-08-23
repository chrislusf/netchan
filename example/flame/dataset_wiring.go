package flame

import (
	"reflect"

	"github.com/chrislusf/netchan/example/lib"
)

func (d *Dataset) EnsureShard(n int) {
	ctype := reflect.ChanOf(reflect.BothDir, d.Type)
	for i := 0; i < n; i++ {
		ds := &DatasetShard{
			Parent:    d,
			ReadChan:  make(chan reflect.Value, 0),
			WriteChan: reflect.MakeChan(ctype, 0),
		}
		d.Shards = append(d.Shards, ds)
	}
}

// hash data or by data key, return a new dataset
func (d *Dataset) Partition(shard int) (ret *Dataset) {
	ret = d.context.newNextDataset(shard, d.Type)
	step := d.context.AddManyToManySourceStep(d, ret)
	step.Function = func(task *Task) {
		for input := range task.InputChan() {
			v := guessKey(input)

			var x int
			switch v.Kind() {
			case reflect.Int:
				x = int(v.Int()) % shard
			case reflect.String:
				x = int(lib.Hash([]byte(v.String()))) % shard
			case reflect.Slice:
				x = int(lib.Hash(v.Bytes())) % shard
			}

			task.Outputs[x].WriteChan.Send(input)
		}
	}
	return
}
