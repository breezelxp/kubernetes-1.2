package scheduler

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/hustcat/go-lib/bitmap"
	"k8s.io/kubernetes/pkg/api"
	priorityutil "k8s.io/kubernetes/plugin/pkg/scheduler/algorithm/priorities/util"
	"strconv"
	"strings"
)

func NumaCpuSelect(pod *api.Pod, node api.Node, pods []*api.Pod) ([]string, []string, error) {
	var (
		numaCpuSet   []string
		normalCpuSet []string
	)
	totalRequestCPU := int64(0)
	for _, container := range pod.Spec.Containers {
		requests := container.Resources.Requests
		totalRequestCPU += requests.Cpu().Value()
	}
	//no cpuset
	if totalRequestCPU == 0 {
		return nil, nil, nil
	}
	nodeCores := priorityutil.GetNonZeroCore(&node.Status.Allocatable)
	numaNode := 2
	if node.Status.NodeInfo.NUMAInfo.Nodes > 0 {
		numaNode = node.Status.NodeInfo.NUMAInfo.Nodes
	}
	cpuMap := bitmap.NewNumaBitmapSize(uint(nodeCores), numaNode)
	for _, existingPod := range pods {
		glog.V(3).Infof("existingPod:%s [cpuset: %s]", existingPod.Name, existingPod.Status.CpuSet)
		set := strings.Split(existingPod.Status.CpuSet, ",")
		for _, c := range set {
			coreNo, _ := strconv.Atoi(c)
			cpuMap.SetBit(uint(coreNo), 1)
		}
	}
	// no numa cpu set
	normalfreeCore := cpuMap.Get0BitOffs()
	if int64(len(normalfreeCore)) >= totalRequestCPU {
		for j := int64(0); j < totalRequestCPU; j++ {
			off := normalfreeCore[j]
			normalCpuSet = append(normalCpuSet, strconv.Itoa(int(off)))
		}
	}
	// numa cpu set
	var (
		numafreeCore [][]uint
		err          error
	)
	if node.Status.NodeInfo.NUMAInfo.Topological == "1" {
		numafreeCore, err = cpuMap.Get0BitOffsNumaVer(uint(numaNode))
	} else {
		numafreeCore, err = cpuMap.Get0BitOffsNuma(uint(numaNode))
	}
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < numaNode; i++ {
		offs := numafreeCore[i]
		if int64(len(offs)) >= totalRequestCPU {
			for j := int64(0); j < totalRequestCPU; j++ {
				off := offs[j]
				//cpuMap.SetBit(off, 1)
				numaCpuSet = append(numaCpuSet, strconv.Itoa(int(off)))
			}
			break
		}
	}
	glog.V(3).Infof("CPUSet select on node(%s) [normalCPU: %+v] [numaCPU: %+v]", node.Name, normalCpuSet, numaCpuSet)
	return normalCpuSet, numaCpuSet, nil
}

func AllocatePodNetwork(pod *api.Pod, node api.Node, pods []*api.Pod) (api.Network, error) {
	// If it is not macvlan, not to allocate network
	if pod.Spec.NetworkMode == api.PodNetworkFlannel {
		return api.Network{}, nil
	}
	var (
		used    bool
		network api.Network
	)
	for _, vm := range node.VMs {
		used = false
		for _, existingPod := range pods {
			if existingPod.Spec.NetworkMode == api.PodNetworkFlannel {
				continue
			}
			if vm.Address == existingPod.Status.Network.Address {
				used = true
				break
			}
		}

		// vm address is specified
		if !used {
			if innerIP, ok := pod.Annotations["scheduler.tencent.cr/inner-ip"]; ok {
				parts := strings.Split(vm.Address, "/")
				if len(parts) <= 0 || innerIP != parts[0] {
					continue
				}
			}
			network.Mode = pod.Spec.NetworkMode
			network.Address = vm.Address
			network.Gateway = vm.Gateway
			network.MacAddress = vm.MacAddress
			network.VlanID = vm.VlanID
			network.Subnet = vm.Subnet
			network.VfID = vm.VfID
			break
		}
	}
	// Network must be allocated
	if used || len(network.Address) == 0 {
		return api.Network{}, fmt.Errorf("Can't find valid vms on node(%s)", node.Name)
	}
	return network, nil
}
