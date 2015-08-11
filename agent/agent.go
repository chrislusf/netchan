// agent.go
package agent

import ()

type AgentServer struct {
	Local  *AgentLocalServer
	leader string
}

func NewAgentServer(localPort int, leader string) *AgentServer {
	a := &AgentServer{leader: leader}
	a.Local = NewAgentLocalServer(localPort)

	err := a.Local.Init()
	if err != nil {
		panic(err)
	}

	return a
}

func (a *AgentServer) Run() {
	a.Local.Run()
}
