package client

import (
	"fmt"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/golang/glog"
)

type Client struct {
	HostPorts string
	// The current syslog-ng node for the service
	Endpoint string
	Conn     net.Conn
}

func (self *Client) connect(endpoint string) error {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", endpoint)
	if err != nil {
		glog.Errorf("failed to resolve tcp address %s: %v", endpoint, err)
		return err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		glog.Errorf("failed to connect to %s: %v", endpoint, err)
		return err
	}

	glog.V(3).Infof("current use of syslog-ng: %s", endpoint)
	self.Conn = conn
	self.Endpoint = endpoint
	return nil
}

func (self *Client) Reconnect() error {
	for _, endpoint := range strings.Split(self.HostPorts, ",") {
		// ignore the endpoint which is not available
		if self.Endpoint == endpoint {
			continue
		}
		if err := self.connect(endpoint); err != nil {
			continue
		} else {
			return nil
		}
	}

	return fmt.Errorf("No syslog-ng server Available")
}

func (self *Client) Close() error {
	self.Conn.Close()
	self.Conn = nil
	return nil
}

func New(hostPorts string) (*Client, error) {
	client := Client{HostPorts: hostPorts}
	rand.Seed(time.Now().UnixNano())
	hostArray := strings.Split(hostPorts, ",")
	index := rand.Intn(len(hostArray))
	if err := client.connect(hostArray[index]); err != nil {
		if err = client.Reconnect(); err != nil {
			return nil, err
		}
	}
	return &client, nil
}
