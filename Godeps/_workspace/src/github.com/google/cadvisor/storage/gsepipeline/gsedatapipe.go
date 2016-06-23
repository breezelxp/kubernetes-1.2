package gsepipeline

import (
	"encoding/json"
	"github.com/golang/glog"
	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/storage"
	gseclient "github.com/google/cadvisor/storage/gsepipeline/client"
	"net"
	"strings"
)

type detailSpec struct {
	Timestamp      int64                `json:"timestamp"`
	MachineName    string               `json:"machine_name,omitempty"`
	ContainerID    string               `json:"container_id,omitempty"`
	ContainerName  string               `json:"container_name,omitempty"`
	ContainerStats *info.ContainerStats `json:"container_stats,omitempty"`
}

type gseStorage struct {
	client      *gseclient.Client
	machineName string
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

func init() {
	storage.RegisterStorageDriver("gsedatapipe", new)
}

func new() (storage.StorageDriver, error) {

	return newGseStorage(*storage.ArgDbHost)
}

func newGseStorage(endpoint string) (*gseStorage, error) {

	client, err := gseclient.New(endpoint)
	if nil != err {
		return nil, err
	}

	client.Connect()
	address, err := getHostETH1Address()
	if err != nil {
		return nil, err
	}

	gseStorageClient := &gseStorage{
		client:      client,
		machineName: address}

	return gseStorageClient, nil

}

func (gse *gseStorage) containerStatsAndDefaultValues(ref info.ContainerReference, stats *info.ContainerStats) *detailSpec {

	timestamp := stats.Timestamp.UnixNano() / 1E3
	var containerName string
	if len(ref.Aliases) > 0 {
		containerName = ref.Aliases[0]
	} else {
		containerName = ref.Name
	}
	detail := &detailSpec{
		Timestamp:      timestamp,
		MachineName:    gse.machineName,
		ContainerID:    ref.Id,
		ContainerName:  containerName,
		ContainerStats: stats,
	}
	return detail
}

func (gse *gseStorage) AddStats(ref info.ContainerReference, stats *info.ContainerStats) error {

	detail := gse.containerStatsAndDefaultValues(ref, stats)
	if detail.ContainerName == "/" || detail.ContainerName == "/docker" || strings.HasPrefix(detail.ContainerName, "k8s_net") {
		return nil
	}
	b, err := json.Marshal(detail)
	if err != nil {
		return err
	}

	err = gse.client.Send(1430, b)
	if err != nil {
		glog.Errorf("%v", err)
		return err
	}
	return nil
}

func (gse *gseStorage) Close() error {

	gse.client.Close()
	return nil
}
