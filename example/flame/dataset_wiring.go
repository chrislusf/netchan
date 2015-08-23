package flame

import (
	"reflect"
)

func (d *Dataset) Shard(n int) *Dataset {
	ctype := reflect.ChanOf(reflect.BothDir, d.Type)
	for i := 0; i < n; i++ {
		ds := &DatasetShard{
			Parent:    d,
			ReadChan:  make(chan reflect.Value, 0),
			WriteChan: reflect.MakeChan(ctype, 0),
		}
		d.Shards = append(d.Shards, ds)
	}
	return d
}

func (d *Dataset) newNextDataset(shardSize int, t reflect.Type) (ret *Dataset) {
	ret = NewDataset(d.context, t)
	ret.Shard(shardSize)
	return
}
