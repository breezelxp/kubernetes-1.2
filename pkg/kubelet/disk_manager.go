/*
Copyright 2015 The Kubernetes Authors All rights reserved.

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

package kubelet

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	cadvisorapi "github.com/google/cadvisor/info/v2"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/util"
	"os/exec"
)

// Manages policy for diskspace management for disks holding docker images and root fs.

// mb is used to easily convert an int to an mb
const mb = 1024 * 1024

// Implementation is thread-safe.
type diskSpaceManager interface {
	// Checks the available disk space
	IsRootDiskSpaceAvailable() (bool, error)
	IsDockerDiskSpaceAvailable() (bool, error)
	SetDiskQuota(pid int, containerName, quotaPath string, storage int64) error
	DeleteDiskQuota(pid int, containerName string) error
}

type DiskSpacePolicy struct {
	// free disk space threshold for filesystem holding docker images.
	DockerFreeDiskMB int
	// free disk space threshold for root filesystem. Host volumes are created on root fs.
	RootFreeDiskMB int
}

type fsInfo struct {
	Usage     int64
	Capacity  int64
	Available int64
	Timestamp time.Time
}

type realDiskSpaceManager struct {
	cadvisor   cadvisor.Interface
	cachedInfo map[string]fsInfo // cache of filesystem info.
	lock       sync.Mutex        // protecting cachedInfo.
	policy     DiskSpacePolicy   // thresholds. Set at creation time.
}

func (dm *realDiskSpaceManager) getFsInfo(fsType string, f func() (cadvisorapi.FsInfo, error)) (fsInfo, error) {
	dm.lock.Lock()
	defer dm.lock.Unlock()
	fsi := fsInfo{}
	if info, ok := dm.cachedInfo[fsType]; ok {
		timeLimit := time.Now().Add(-2 * time.Second)
		if info.Timestamp.After(timeLimit) {
			fsi = info
		}
	}
	if fsi.Timestamp.IsZero() {
		fs, err := f()
		if err != nil {
			return fsInfo{}, err
		}
		fsi.Timestamp = time.Now()
		fsi.Usage = int64(fs.Usage)
		fsi.Capacity = int64(fs.Capacity)
		fsi.Available = int64(fs.Available)
		dm.cachedInfo[fsType] = fsi
	}
	return fsi, nil
}

func (dm *realDiskSpaceManager) IsDockerDiskSpaceAvailable() (bool, error) {
	return dm.isSpaceAvailable("docker", dm.policy.DockerFreeDiskMB, dm.cadvisor.DockerImagesFsInfo)
}

func (dm *realDiskSpaceManager) IsRootDiskSpaceAvailable() (bool, error) {
	return dm.isSpaceAvailable("root", dm.policy.RootFreeDiskMB, dm.cadvisor.RootFsInfo)
}

func (dm *realDiskSpaceManager) isSpaceAvailable(fsType string, threshold int, f func() (cadvisorapi.FsInfo, error)) (bool, error) {
	fsInfo, err := dm.getFsInfo(fsType, f)
	if err != nil {
		return true, fmt.Errorf("failed to get fs info for %q: %v", fsType, err)
	}
	if fsInfo.Capacity == 0 {
		return true, fmt.Errorf("could not determine capacity for %q fs. Info: %+v", fsType, fsInfo)
	}
	if fsInfo.Available < 0 {
		return true, fmt.Errorf("wrong available space for %q: %+v", fsType, fsInfo)
	}

	if fsInfo.Available < int64(threshold)*mb {
		glog.Infof("Running out of space on disk for %q: available %d MB, threshold %d MB", fsType, fsInfo.Available/mb, threshold)
		return false, nil
	}
	return true, nil
}

func validatePolicy(policy DiskSpacePolicy) error {
	if policy.DockerFreeDiskMB < 0 {
		return fmt.Errorf("free disk space should be non-negative. Invalid value %d for docker disk space threshold.", policy.DockerFreeDiskMB)
	}
	if policy.RootFreeDiskMB < 0 {
		return fmt.Errorf("free disk space should be non-negative. Invalid value %d for root disk space threshold.", policy.RootFreeDiskMB)
	}
	return nil
}

func (dm *realDiskSpaceManager) SetDiskQuota(pid int, containerName, quotaPath string, storage int64) error {
	dm.lock.Lock()
	defer dm.lock.Unlock()
	if storage <= 0 {
		glog.Infof("Container(%s) storage is less than zero, do not need to set quota", containerName)
		return nil
	}
	projid := pid % 0xFFFF
	glog.V(3).Infof("Handle for SetDiskQuota:Pid=>%d(c32:%d) Name=>%s Storage=>%d", pid, projid, containerName, storage)
	// set /etc/projects file
	if err := util.ChangeXFSProject("/etc/projects", fmt.Sprintf("%d:%s", projid, quotaPath), fmt.Sprintf("^%d:", projid)); err != nil {
		return err
	}
	// set /etc/projid file
	if err := util.ChangeXFSProject("/etc/projid", fmt.Sprintf("%s-%d:%d", containerName, projid, projid), fmt.Sprintf(":%d$", projid)); err != nil {
		return err
	}
	// xfs_quota
	if _, err := exec.Command("xfs_quota", "-x", "-c", fmt.Sprintf("project -s %s-%d", containerName, projid), "/data").CombinedOutput(); err != nil {
		return err
	}
	if _, err := exec.Command("xfs_quota", "-x", "-c", fmt.Sprintf("limit -p bhard=%dg %s-%d", storage, containerName, projid), "/data").CombinedOutput(); err != nil {
		return err
	}
	return nil
}

func (dm *realDiskSpaceManager) DeleteDiskQuota(pid int, containerName string) error {
	dm.lock.Lock()
	defer dm.lock.Unlock()
	projid := pid % 0xFFFF
	glog.V(3).Infof("Handle for CleanDiskQuota:Pid=>%d(c32:%d) Name=>%s", pid, projid, containerName)
	cmd := exec.Command("xfs_quota", "-x", "-c", fmt.Sprintf("project -C %s-%d", containerName, projid), "/data")
	stderr := bytes.NewBuffer(nil)
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		errStr := string(stderr.Bytes())
		glog.V(3).Infof("Exec Command failed,stderr: %s", errStr)
		if strings.Contains(errStr, "doesn't exist") || strings.Contains(errStr, "no such ") {
			return nil
		}
		return err
	}
	// set /etc/projects file
	if err := util.ChangeXFSProject("/etc/projects", "", fmt.Sprintf("^%d:", projid)); err != nil {
		return err
	}
	// set /etc/projid file
	if err := util.ChangeXFSProject("/etc/projid", "", fmt.Sprintf(":%d$", projid)); err != nil {
		return err
	}
	return nil
}

func newDiskSpaceManager(cadvisorInterface cadvisor.Interface, policy DiskSpacePolicy) (diskSpaceManager, error) {
	// validate policy
	err := validatePolicy(policy)
	if err != nil {
		return nil, err
	}

	dm := &realDiskSpaceManager{
		cadvisor:   cadvisorInterface,
		policy:     policy,
		cachedInfo: map[string]fsInfo{},
	}

	return dm, nil
}
