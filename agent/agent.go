// agent.go
package agent

import ()

type AgentServer struct {
	Local  *AgentLocalServer
	leader string
}

func NewAgentServer(dir string, localPort int, leader string) *AgentServer {
	a := &AgentServer{leader: leader}
	a.Local = NewAgentLocalServer(dir, localPort)

	err := a.Local.Init()
	if err != nil {
		panic(err)
	}

	return a
}

func (a *AgentServer) Run() {
	a.Local.Run()
}
