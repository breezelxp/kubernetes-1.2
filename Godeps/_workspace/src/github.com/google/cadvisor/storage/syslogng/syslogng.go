package syslogng

import (
	"encoding/json"
	"github.com/golang/glog"
	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/storage"
	client "github.com/google/cadvisor/storage/syslogng/client"
	"net"
	"strings"
)

func init() {
	storage.RegisterStorageDriver("syslogng", new)
}

type syslogngStorage struct {
	client      *client.Client
	machineName string
}

type detailSpec struct {
	Timestamp      int64                `json:"timestamp"`
	MachineName    string               `json:"machine_name,omitempty"`
	ContainerID    string               `json:"container_id,omitempty"`
	ContainerName  string               `json:"container_name,omitempty"`
	ContainerStats *info.ContainerStats `json:"container_stats,omitempty"`
}

func new() (storage.StorageDriver, error) {
	return newStorage(*storage.ArgDbHost)
}

func getHostETH1Address() (string, error) {
	ief, err := net.InterfaceByName("eth1")
	if err != nil {
		return "", err
	}
	addr, err := ief.Addrs()
	if err != nil {
		return "", err
	}
	return addr[0].(*net.IPNet).IP.String(), nil
}

func (self *syslogngStorage) containerStatsAndDefaultValues(ref info.ContainerReference, stats *info.ContainerStats) *detailSpec {
	timestamp := stats.Timestamp.UnixNano() / 1E3
	var containerName string
	if len(ref.Aliases) > 0 {
		containerName = ref.Aliases[0]
	} else {
		containerName = ref.Name
	}
	detail := &detailSpec{
		Timestamp:      timestamp,
		MachineName:    self.machineName,
		ContainerID:    ref.Id,
		ContainerName:  containerName,
		ContainerStats: stats,
	}
	return detail
}

func (self *syslogngStorage) AddStats(ref info.ContainerReference, stats *info.ContainerStats) error {
	detail := self.containerStatsAndDefaultValues(ref, stats)
	if detail.ContainerName == "/" || detail.ContainerName == "/docker" || strings.HasPrefix(detail.ContainerName, "k8s_net") {
		return nil
	}
	b, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	msg := []byte("docker," + self.machineName + ",1024 ")
	msg = append(msg, b...)
	msg = append(msg, []byte("\r\n")...)
	if self.client.Conn == nil {
		if err = self.client.Reconnect(); err != nil {
			return err
		}
	}
	_, err = self.client.Conn.Write(msg)
	if err != nil {
		glog.Errorf("++ TEST: %v", err)
		return err
	}
	return nil
}

func (self *syslogngStorage) Close() error {
	self.client.Close()
	self.client = nil
	return nil
}

func newStorage(hostPorts string) (*syslogngStorage, error) {
	syslogngClient, err := client.New(hostPorts)
	if err != nil {
		return nil, err
	}
	address, err := getHostETH1Address()
	if err != nil {
		return nil, err
	}
	syslogngStorage := &syslogngStorage{
		client:      syslogngClient,
		machineName: address,
	}
	return syslogngStorage, nil
}
