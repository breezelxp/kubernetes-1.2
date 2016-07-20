package main

import (
	"fmt"
	gseclient "github.com/google/cadvisor/storage/gsepipeline/client"
)

func main() {

	gsestorage, _ := gseclient.New("/usr/local/gse/gseagent/ipc.state.report")
	fmt.Println(gsestorage.Connect())

	fmt.Println(gsestorage.Send(1430, []byte("uriwangtest")))

	fmt.Println("sended")

}
