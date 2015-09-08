package flame

import (
	"reflect"
)

// reference by task runner on remote mode
var Contexts []*FlowContext

type FlowContext struct {
	Id       int
	Steps    []*Step
	Datasets []*Dataset
}

func NewContext() (fc *FlowContext) {
	fc = &FlowContext{Id: len(Contexts)}
	Contexts = append(Contexts, fc)
	return
}

func (fc *FlowContext) newNextDataset(shardSize int, t reflect.Type) (ret *Dataset) {
	if t != nil {
		ret = NewDataset(fc, t)
		ret.SetupShard(shardSize)
	}
	return
}

// the tasks should run on the source dataset shard
func (f *FlowContext) AddOneToOneStep(input *Dataset, output *Dataset) (s *Step) {
	s = &Step{Output: output, Id: len(f.Steps)}
	s.Inputs = append(s.Inputs, input)
	// setup the network
	for i, shard := range input.GetShards() {
		t := &Task{Inputs: []*DatasetShard{shard}, Step: s, Id: i}
		if output != nil {
			t.Outputs = []*DatasetShard{output.GetShards()[i]}
		}
		s.Tasks = append(s.Tasks, t)
	}
	f.Steps = append(f.Steps, s)
	return
}

// the task should run on the destination dataset shard
func (f *FlowContext) AddAllToOneStep(input *Dataset, output *Dataset) (s *Step) {
	s = &Step{Output: output, Id: len(f.Steps)}
	s.Inputs = append(s.Inputs, input)
	// setup the network
	t := &Task{Step: s, Id: 0}
	if output != nil {
		t.Outputs = []*DatasetShard{output.GetShards()[0]}
	}
	for _, shard := range input.GetShards() {
		t.Inputs = append(t.Inputs, shard)
	}
	s.Tasks = append(s.Tasks, t)
	f.Steps = append(f.Steps, s)
	return
}

// the task should run on the source dataset shard
// input is nil for initial source dataset
func (f *FlowContext) AddOneToAllStep(input *Dataset, output *Dataset) (s *Step) {
	s = &Step{Output: output, Id: len(f.Steps)}
	s.Inputs = append(s.Inputs, input)
	// setup the network
	t := &Task{Step: s, Id: 0}
	if input != nil {
		t.Inputs = []*DatasetShard{input.GetShards()[0]}
	}
	for _, shard := range output.GetShards() {
		t.Outputs = append(t.Outputs, shard)
	}
	s.Tasks = append(s.Tasks, t)
	f.Steps = append(f.Steps, s)
	return
}

// the tasks should run on the source dataset shards
func (f *FlowContext) AddAllToAllStep(input *Dataset, output *Dataset) (s *Step) {
	s = &Step{Output: output, Id: len(f.Steps)}
	s.Inputs = append(s.Inputs, input)
	// setup the network
	for _, inShard := range input.GetShards() {
		t := &Task{Inputs: []*DatasetShard{inShard}, Step: s, Id: 0}
		for _, outShard := range output.GetShards() {
			t.Outputs = append(t.Outputs, outShard)
		}
		s.Tasks = append(s.Tasks, t)
	}
	f.Steps = append(f.Steps, s)
	return
}

func (f *FlowContext) AddOneToEveryNStep(input *Dataset, n int, output *Dataset) (s *Step) {
	s = &Step{Output: output, Id: len(f.Steps)}
	s.Inputs = append(s.Inputs, input)
	// setup the network
	m := len(input.GetShards())
	for i, inShard := range input.GetShards() {
		t := &Task{Inputs: []*DatasetShard{inShard}, Step: s, Id: len(s.Tasks)}
		for k := 0; k < n; k++ {
			t.Outputs = append(t.Outputs, output.GetShards()[k*m+i])
		}
		s.Tasks = append(s.Tasks, t)
	}
	f.Steps = append(f.Steps, s)
	return
}

func (f *FlowContext) AddEveryNToOneStep(input *Dataset, m int, output *Dataset) (s *Step) {
	s = &Step{Output: output, Id: len(f.Steps)}
	s.Inputs = append(s.Inputs, input)
	// setup the network
	n := len(output.GetShards())
	for i, outShard := range output.GetShards() {
		t := &Task{Outputs: []*DatasetShard{outShard}, Step: s, Id: len(s.Tasks)}
		for k := 0; k < m; k++ {
			t.Inputs = append(t.Inputs, input.GetShards()[k*n+i])
		}
		s.Tasks = append(s.Tasks, t)
	}
	f.Steps = append(f.Steps, s)
	return
}

// All dataset should have the same number of shards.
func (f *FlowContext) MergeDatasets1ShardTo1Step(inputs []*Dataset, output *Dataset) (s *Step) {
	s = &Step{Output: output, Id: len(f.Steps)}
	for _, input := range inputs {
		s.Inputs = append(s.Inputs, input)
	}
	// setup the network
	if output != nil {
		for shardId, outShard := range output.Shards {
			t := &Task{Step: s, Id: shardId}
			for _, input := range inputs {
				t.Inputs = append(t.Inputs, input.GetShards()[shardId])
			}
			if output != nil {
				t.Outputs = append(t.Outputs, outShard)
			}
			s.Tasks = append(s.Tasks, t)
		}
	}
	f.Steps = append(f.Steps, s)
	return
}
