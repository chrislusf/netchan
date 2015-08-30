package driver

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/chrislusf/netchan/example/driver/cmd"
	"github.com/chrislusf/netchan/service_discovery/client"
	"github.com/chrislusf/netchan/util"
	"github.com/golang/protobuf/proto"
)

func RemoteExecute(leader string, agentName string, command *cmd.ControlMessage) error {
	// println("running on", agentName, "...")
	conn, err := getCommandConnection(leader, agentName)
	if err != nil {
		return err
	}
	defer conn.Close()

	buf := make([]byte, 4)

	// serialize the commend
	data, err := proto.Marshal(command)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	// send the command
	if err = util.WriteData(conn, buf, []byte("CMD "), data); err != nil {
		println("failed to write to", agentName, ":", err.Error())
		return err
	}

	// read output and print it to stdout
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		fmt.Printf("%s>%s\n", agentName, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("Failed to scan output: %v", err)
	}

	return err

}

func getCommandConnection(leader string, agentName string) (net.Conn, error) {
	l := client.NewNameServiceAgent(leader)

	// looking for the agentName
	var target string
	for {
		locations := l.Find(agentName)
		if len(locations) > 0 {
			target = locations[0]
		}
		if target != "" {
			break
		} else {
			time.Sleep(time.Second)
			print("z")
		}
	}

	// connect to a TCP server
	network := "tcp"
	raddr, err := net.ResolveTCPAddr(network, target)
	if err != nil {
		return nil, fmt.Errorf("Fail to resolve %s:%v", target, err)
	}

	// println("dial tcp", raddr.String())
	conn, err := net.DialTCP(network, nil, raddr)
	if err != nil {
		return nil, fmt.Errorf("Fail to dial %s:%v", raddr, err)
	}

	return conn, err
}
