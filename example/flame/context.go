package flame

import ()

type FlowContext struct {
	Steps []*Step
}

func (f *FlowContext) AddOneToOneStep(input *Dataset, output *Dataset) (s *Step) {
	s = &Step{Input: input, Output: output, Id: len(f.Steps)}
	// setup the network
	for i, shard := range input.GetShards() {
		t := &Task{Inputs: []*DatasetShard{shard}, Outputs: []*DatasetShard{output.GetShards()[i]}, Step: s, Id: i}
		s.Tasks = append(s.Tasks, t)
	}
	f.Steps = append(f.Steps, s)
	return
}

func (f *FlowContext) AddManyToOneStep(input *Dataset, output *Dataset) (s *Step) {
	s = &Step{Input: input, Output: output, Id: len(f.Steps)}
	// setup the network
	t := &Task{Outputs: []*DatasetShard{output.GetShards()[0]}, Step: s, Id: 0}
	for _, shard := range input.GetShards() {
		t.Inputs = append(t.Inputs, shard)
	}
	s.Tasks = append(s.Tasks, t)
	f.Steps = append(f.Steps, s)
	return
}
