package scheduler

import ()

type Scheduler struct {
	EventChan chan interface{}
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		EventChan: make(chan interface{}),
	}
}

func (s *Scheduler) Loop() {
	for {
		event := <-s.EventChan
		switch event := event.(type) {
		default:
		case SubmittedJob:
			event.ContextId += 1
		case int:
		case *bool:
		case *int:
		}
	}
}
