package apiserver

import (
	"encoding/json"
	//"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
)

type HeartReportor struct {
	BcsApi string
}

type Heart struct {
	ApiAddrr   string `json:"addr"`
	ApiPort    int    `json:"port"`
	ReportTime string `json:"reporttime"`
}

func (r *HeartReportor) StartReport(addr string, port int) {
	go r.PollReportHeart(addr, port)
}

func (r *HeartReportor) PollReportHeart(addr string, port int) error {
	url := r.BcsApi + "/ccapi/v1/editk8s/apiserver/" + addr + ":" + strconv.Itoa(port) + "/add"

	heart := new(Heart)
	heart.ApiAddrr = addr
	heart.ApiPort = port

	ticker := time.NewTicker(time.Second * 30)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.Report(heart, url)
		}
	}

	return nil
}

func (r *HeartReportor) Report(heart *Heart, url string) error {
	heart.ReportTime = strconv.FormatInt(time.Now().Unix(), 10)
	data, err := json.Marshal(heart)
	if err != nil {
		glog.Warning("marshal heart report data failed!err:%s", err.Error())
		return err
	}

	req, err := http.NewRequest("POST", url, strings.NewReader(string(data)))
	if err != nil {
		glog.Error("heart report failed!err:%s", err.Error())
		return err
	}

	client := &http.Client{}

	rsp, err := client.Do(req)
	if err != nil {
		glog.Error("heart report failed!err:%s", err.Error())
		return err
	}

	defer rsp.Body.Close()

	return nil
}
