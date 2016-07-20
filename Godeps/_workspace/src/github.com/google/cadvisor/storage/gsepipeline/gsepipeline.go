package gsepipeline

import (
	"encoding/json"
	"github.com/golang/glog"
	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/storage"
	gseclient "github.com/google/cadvisor/storage/gsepipeline/client"
	kubeletclient "github.com/google/cadvisor/storage/gsepipeline/kubelet"
	kube_client "k8s.io/kubernetes/pkg/kubelet/client"
	"net"
	"os"
	"strings"
)

type detailSpec struct {
	Timestamp      int64                   `json:"timestamp"`
	MachineName    string                  `json:"machine_name,omitempty"`
	ContainerID    string                  `json:"container_id,omitempty"`
	ContainerName  string                  `json:"container_name,omitempty"`
	ContainerInfo  info.ContainerReference `json:"container_info,omitempty"`
	ContainerStats *info.ContainerStats    `json:"container_stats,omitempty"`
}

type gseStorage struct {
	client      *gseclient.Client
	dataid      uint64
	host        kubeletclient.Host
	machineName string
	kubeletcli  *kubeletclient.KubeletClient
}

func init() {
	storage.RegisterStorageDriver("gsedatapipe", new)
}

func new() (storage.StorageDriver, error) {

	return newGseStorage(*storage.ArgDbHost, *storage.ArgKubeletIp, *storage.ArgKubeletPort, *storage.ArgDataId)
}

func getHostETH1Address() (string, error) {

	hostName, _ := os.Hostname()
	ief, err := net.InterfaceByName("eth1")
	if err != nil {
		return hostName, err
	}
	addr, err := ief.Addrs()
	if err != nil {
		return hostName, err
	}
	return addr[0].(*net.IPNet).IP.String(), nil
}

func newGseStorage(endpoint string, kubeletip string, kubeletport int, dataid uint64) (*gseStorage, error) {

	glog.V(0).Info("endpoint:", endpoint, "kubelet ip:", kubeletip, "kubelet port:", kubeletport, "dataid:", dataid)

	client, err := gseclient.New(endpoint)
	if nil != err {
		return nil, err
	}

	client.Connect()

	config := &kube_client.KubeletClientConfig{EnableHttps: false}
	kubeletd, err := kubeletclient.NewKubeletClient(config)

	address, err := getHostETH1Address()
	if err != nil {
		glog.Error(err)
	}

	if nil == err {
		gseStorageClient := &gseStorage{
			client:      client,
			kubeletcli:  kubeletd,
			dataid:      dataid,
			machineName: address,
			host:        kubeletclient.Host{IP: kubeletip, Port: kubeletport}}

		return gseStorageClient, nil
	}

	return nil, err

}

func (gse *gseStorage) AddStats(ref info.ContainerReference, stats *info.ContainerStats) error {

	timestamp := stats.Timestamp.UnixNano()

	var containerName string
	if len(ref.Aliases) > 0 {
		containerName = ref.Aliases[0]
	} else {
		containerName = ref.Name
	}

	detail := &detailSpec{
		MachineName:    gse.machineName,
		ContainerID:    ref.Id,
		ContainerName:  containerName,
		Timestamp:      timestamp,
		ContainerStats: stats}

	detail.ContainerInfo.Id = ref.Id
	detail.ContainerInfo.Name = ref.Name
	detail.ContainerInfo.Namespace = ref.Namespace
	if nil != ref.Aliases {
		detail.ContainerInfo.Aliases = ref.Aliases
	}

	labels := make(map[string]string)

	for k, v := range ref.Labels {
		labels[strings.Replace(k, ".", "_", -1)] = v
	}
	if nil != labels {
		detail.ContainerInfo.Labels = labels
	}

	if detail.ContainerName == "/" || detail.ContainerName == "/docker" || strings.HasPrefix(detail.ContainerName, "k8s_net") {
		return nil
	}
	b, err := json.Marshal(detail)
	if err != nil {
		return err
	}

	err = gse.client.Send(uint32(gse.dataid), b)
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
