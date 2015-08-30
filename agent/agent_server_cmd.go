package agent

import (
	"log"
	"os"
	"os/exec"

	"github.com/chrislusf/netchan/example/driver/cmd"
)

func (as *AgentServer) handleCommandConnection(
	command *cmd.ControlMessage) *cmd.ControlMessage {
	reply := &cmd.ControlMessage{}
	if command.GetType() == cmd.ControlMessage_StartRequest {
		*reply.Type = cmd.ControlMessage_StartResponse
		reply.StartResponse = as.handleStart(command.StartRequest)
	}
	return reply
}

func (as *AgentServer) handleStart(
	startRequest *cmd.StartRequest) *cmd.StartResponse {
	reply := &cmd.StartResponse{}

	cmd := exec.Command(
		*startRequest.Path,
		startRequest.Args...,
	)
	cmd.Env = startRequest.Envs
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		log.Printf("Failed to start commend %s: %v", *startRequest.Path, err)
		*reply.Error = err.Error()
	} else {
		*reply.Pid = int32(cmd.Process.Pid)
	}

	return reply
}
