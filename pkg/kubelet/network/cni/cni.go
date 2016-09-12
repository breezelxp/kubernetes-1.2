/*
Copyright 2014 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cni

import (
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/appc/cni/libcni"
	cnitypes "github.com/appc/cni/pkg/types"
	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/api"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/dockertools"
	"k8s.io/kubernetes/pkg/kubelet/network"
	"k8s.io/kubernetes/pkg/util/sets"
	"k8s.io/kubernetes/pkg/util/wait"
)

const (
	CNIPluginName        = "cni"
	DefaultNetDir        = "/etc/cni/net.d"
	DefaultCNIDir        = "/opt/cni/bin"
	VendorCNIDirTemplate = "%s/opt/%s/bin"
)

type cniNetworkPlugin struct {
	defaultNetwork *cniNetwork
	host           network.Host
	sync.RWMutex
	killing sets.String
}

type cniNetwork struct {
	name          string
	NetworkConfig *libcni.NetworkConfig
	CNIConfig     *libcni.CNIConfig
}

func probeNetworkPluginsWithVendorCNIDirPrefix(pluginDir, vendorCNIDirPrefix string) []network.NetworkPlugin {
	configList := make([]network.NetworkPlugin, 0)
	network, err := getDefaultCNINetwork(pluginDir, vendorCNIDirPrefix)
	if err != nil {
		return configList
	}
	return append(configList, &cniNetworkPlugin{
		defaultNetwork: network,
		killing:        sets.NewString(),
	})
}

func ProbeNetworkPlugins(pluginDir string) []network.NetworkPlugin {
	return probeNetworkPluginsWithVendorCNIDirPrefix(pluginDir, "")
}

func getDefaultCNINetwork(pluginDir, vendorCNIDirPrefix string) (*cniNetwork, error) {
	if pluginDir == "" {
		pluginDir = DefaultNetDir
	}
	files, err := libcni.ConfFiles(pluginDir)
	switch {
	case err != nil:
		return nil, err
	case len(files) == 0:
		return nil, nil
	}

	sort.Strings(files)
	for _, confFile := range files {
		conf, err := libcni.ConfFromFile(confFile)
		if err != nil {
			glog.Warningf("Error loading CNI config file %s: %v", confFile, err)
			continue
		}
		// Search for vendor-specific plugins as well as default plugins in the CNI codebase.
		vendorCNIDir := fmt.Sprintf(VendorCNIDirTemplate, vendorCNIDirPrefix, conf.Network.Type)
		cninet := &libcni.CNIConfig{
			Path: []string{DefaultCNIDir, vendorCNIDir},
		}
		network := &cniNetwork{name: conf.Network.Name, NetworkConfig: conf, CNIConfig: cninet}
		return network, nil
	}
	return nil, fmt.Errorf("No valid networks found in %s", pluginDir)
}

func getPodCNINetwork(pod *api.Pod) (*cniNetwork, error) {
	if (pod == nil || pod.Status.Network == api.Network{}) {
		return nil, fmt.Errorf("Can't get pod network: %v", pod)
	}
	netconf := map[string]interface{}{
		"name":   pod.Namespace + "-" + pod.Name,
		"type":   pod.Spec.NetworkMode,
		"master": network.Eth1InterfaceName,
		"mac":    pod.Status.Network.MacAddress,
		"vf":     pod.Status.Network.VfID,
		"vlan":   pod.Status.Network.VlanID,
		"ipam": map[string]interface{}{
			"type":    "txipam",
			"subnet":  pod.Status.Network.Subnet,
			"gateway": pod.Status.Network.Gateway,
			"routes": []interface{}{
				map[string]interface{}{
					"dst": "0.0.0.0/0",
				},
				map[string]interface{}{
					"dst": "10.0.0.0/8",
				},
			},
		},
	}
	b, err := json.Marshal(netconf)
	if err != nil {
		return nil, err
	}
	conf, err := libcni.ConfFromBytes(b)
	if err != nil {
		return nil, err
	}
	vendorCNIDir := fmt.Sprintf(VendorCNIDirTemplate, "", conf.Network.Type)
	cninet := &libcni.CNIConfig{
		Path: []string{DefaultCNIDir, vendorCNIDir},
	}
	network := &cniNetwork{name: conf.Network.Name, NetworkConfig: conf, CNIConfig: cninet}
	return network, nil
}

func (plugin *cniNetworkPlugin) getDefaultNetwork() *cniNetwork {
	plugin.RLock()
	defer plugin.RUnlock()
	return plugin.defaultNetwork
}

func (plugin *cniNetworkPlugin) setDefaultNetwork(n *cniNetwork) {
	plugin.Lock()
	defer plugin.Unlock()
	plugin.defaultNetwork = n
}

func (plugin *cniNetworkPlugin) checkInitialized() error {
	if plugin.getDefaultNetwork() == nil {
		return fmt.Errorf("cni config unintialized")
	}
	return nil
}

func (plugin *cniNetworkPlugin) syncKillingCleanups() {
	runtime, ok := plugin.host.GetRuntime().(*dockertools.DockerManager)
	if !ok {
		return
	}
	plugin.RLock()
	defer plugin.RUnlock()
	for _, id := range plugin.killing.List() {
		if !runtime.CheckContainerExists(id) {
			fmt.Printf("Delete killing: %v", id)
			plugin.killing.Delete(id)
		}
	}
}

func (plugin *cniNetworkPlugin) Init(host network.Host) error {
	plugin.host = host
	// clean the killing sets
	go wait.Forever(func() {
		plugin.syncKillingCleanups()
	}, 8*time.Hour)
	return nil
}

func (plugin *cniNetworkPlugin) Event(name string, details map[string]interface{}) {
	pod, ok := details[name].(*api.Pod)
	if !ok {
		glog.Warningf("%s event didn't contain pod network", name)
		return
	}
	if cniNetwork, err := getPodCNINetwork(pod); err != nil {
		glog.Errorf("Failed to get cninetwork config: %v", err)
		return
	} else {
		plugin.setDefaultNetwork(cniNetwork)
	}
}

func (plugin *cniNetworkPlugin) Name() string {
	return CNIPluginName
}

func (plugin *cniNetworkPlugin) SetUpPod(namespace string, name string, id kubecontainer.DockerID) error {
	runtime, ok := plugin.host.GetRuntime().(*dockertools.DockerManager)
	if !ok {
		return fmt.Errorf("CNI execution called on non-docker runtime")
	}
	netns, err := runtime.GetNetNS(id.ContainerID())
	if err != nil {
		return err
	}

	pod, ok := plugin.host.GetPodByName(namespace, name)
	if !ok {
		return fmt.Errorf("Failed to find pod %s_%s", name, namespace)
	}
	ip, _, err := net.ParseCIDR(pod.Status.Network.Address)
	if err != nil {
		return err
	}

	cniNetwork, err := getPodCNINetwork(pod)
	if err != nil {
		return fmt.Errorf("Failed to get pod's (%s_%s) cninetwork config: %v", name, namespace, err)
	}
	_, err = cniNetwork.addToNetwork(name, namespace, id.ContainerID(), netns, ip.String())
	if err != nil {
		glog.Errorf("Error while adding to cni network: %s", err)
		return err
	}

	return err
}

func (plugin *cniNetworkPlugin) TearDownPod(namespace string, name string, id kubecontainer.DockerID, details interface{}) error {
	if plugin.killing.Has(id.ContainerID().ID) {
		glog.V(4).Infof("The pod (%s_%s) is already in killing", name, namespace)
		return nil
	}
	plugin.Lock()
	plugin.killing.Insert(id.ContainerID().ID)
	plugin.Unlock()
	runtime, ok := plugin.host.GetRuntime().(*dockertools.DockerManager)
	if !ok {
		return fmt.Errorf("CNI execution called on non-docker runtime")
	}
	netns, err := runtime.GetNetNS(id.ContainerID())
	if err != nil {
		return err
	}
	pod := details.(*api.Pod)
	cniNetwork, err := getPodCNINetwork(pod)
	if err != nil {
		return fmt.Errorf("Failed to get pod's (%s_%s) cninetwork config: %v", name, namespace, err)
	}
	return cniNetwork.deleteFromNetwork(name, namespace, id.ContainerID(), netns)
}

// TODO: Use the addToNetwork function to obtain the IP of the Pod. That will assume idempotent ADD call to the plugin.
// Also fix the runtime's call to Status function to be done only in the case that the IP is lost, no need to do periodic calls
func (plugin *cniNetworkPlugin) Status(namespace string, name string, id kubecontainer.DockerID) (*network.PodNetworkStatus, error) {
	runtime, ok := plugin.host.GetRuntime().(*dockertools.DockerManager)
	if !ok {
		return nil, fmt.Errorf("CNI execution called on non-docker runtime")
	}
	ipStr, err := runtime.GetContainerIP(string(id), network.DefaultInterfaceName)
	if err != nil {
		return nil, err
	}
	ip, _, err := net.ParseCIDR(strings.Trim(ipStr, "\n"))
	if err != nil {
		return nil, err
	}
	return &network.PodNetworkStatus{IP: ip}, nil
}

func (network *cniNetwork) addToNetwork(podName string, podNamespace string, podInfraContainerID kubecontainer.ContainerID, podNetnsPath, podIP string) (*cnitypes.Result, error) {
	rt, err := buildCNIRuntimeConf(podName, podNamespace, podInfraContainerID, podNetnsPath, podIP)
	if err != nil {
		glog.Errorf("Error adding network: %v", err)
		return nil, err
	}

	netconf, cninet := network.NetworkConfig, network.CNIConfig
	glog.V(4).Infof("About to run with conf.Network.Type=%v, c.Path=%v, conf.Bytes=%s, rt=%v", netconf.Network.Type, cninet.Path, string(netconf.Bytes), rt)
	res, err := cninet.AddNetwork(netconf, rt)
	if err != nil {
		glog.Errorf("Error adding network: %v", err)
		return nil, err
	}

	return res, nil
}

func (network *cniNetwork) deleteFromNetwork(podName string, podNamespace string, podInfraContainerID kubecontainer.ContainerID, podNetnsPath string) error {
	rt, err := buildCNIRuntimeConf(podName, podNamespace, podInfraContainerID, podNetnsPath, "")
	if err != nil {
		glog.Errorf("Error deleting network: %v", err)
		return err
	}

	netconf, cninet := network.NetworkConfig, network.CNIConfig
	glog.V(4).Infof("About to run with conf.Network.Type=%v, c.Path=%v, conf.Bytes=%s, rt=%v", netconf.Network.Type, cninet.Path, string(netconf.Bytes), rt)
	err = cninet.DelNetwork(netconf, rt)
	if err != nil {
		glog.Errorf("Error deleting network: %v", err)
		return err
	}
	return nil
}

func buildCNIRuntimeConf(podName string, podNs string, podInfraContainerID kubecontainer.ContainerID, podNetnsPath, podIP string) (*libcni.RuntimeConf, error) {
	glog.V(4).Infof("Got netns path %v", podNetnsPath)
	glog.V(4).Infof("Using netns path %v", podNs)

	rt := &libcni.RuntimeConf{
		ContainerID: podInfraContainerID.ID,
		NetNS:       podNetnsPath,
		IfName:      network.Eth1InterfaceName,
		Args: [][2]string{
			//			{"K8S_POD_NAMESPACE", podNs},
			//			{"K8S_POD_NAME", podName},
			//			{"K8S_POD_INFRA_CONTAINER_ID", podInfraContainerID.ID},
			{"IP", podIP},
		},
	}

	return rt, nil
}
