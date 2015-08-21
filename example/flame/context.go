package flame

import ()

type FlowContext struct {
	Steps []*Step
}

func (f *FlowContext) AddStep(input *Dataset, output *Dataset) (s *Step) {
	s = &Step{Input: input, Output: output, Id: len(f.Steps)}
	// setup the network
	for i, shard := range input.GetShards() {
		t := &Task{Inputs: []*DatasetShard{shard}, Outputs: []*DatasetShard{output.GetShards()[i]}, Step: s, Id: i}
		s.Tasks = append(s.Tasks, t)
	}
	f.Steps = append(f.Steps, s)
	return
}
