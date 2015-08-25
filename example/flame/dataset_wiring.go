package flame

import (
	"reflect"

	"github.com/chrislusf/netchan/example/lib"
)

func (d *Dataset) EnsureShard(n int) {
	ctype := reflect.ChanOf(reflect.BothDir, d.Type)
	for i := 0; i < n; i++ {
		ds := &DatasetShard{
			Id:        i,
			Parent:    d,
			ReadChan:  make(chan reflect.Value, 0),
			WriteChan: reflect.MakeChan(ctype, 0),
		}
		d.Shards = append(d.Shards, ds)
	}
}

// hash data or by data key, return a new dataset
func (d *Dataset) Partition(shard int) *Dataset {
	return d.partition_scatter(shard).partition_collect(shard)
}

func (d *Dataset) partition_scatter(shard int) (ret *Dataset) {
	ret = d.context.newNextDataset(len(d.Shards)*shard, d.Type)
	step := d.context.AddOneToEveryNStep(d, shard, ret)
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
		for _, out := range task.Outputs {
			out.WriteChan.Close()
		}
	}
	return
}

func (d *Dataset) partition_collect(shard int) (ret *Dataset) {
	m := len(d.Shards) / shard
	ret = d.context.newNextDataset(shard, d.Type)
	step := d.context.AddEveryNToOneStep(d, m, ret)
	step.Function = func(task *Task) {
		for input := range task.InputChan() {
			task.Outputs[0].WriteChan.Send(input)
		}
		task.Outputs[0].WriteChan.Close()
	}
	return
}
