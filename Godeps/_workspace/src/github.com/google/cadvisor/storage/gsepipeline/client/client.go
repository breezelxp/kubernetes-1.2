package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/golang/glog"
	"net"
	"time"
)

type dataHead struct {
	msgType uint32
	dataID  uint32
	utctime uint32
	bodylen uint32
	resv    [2]uint32
}

type AsyncProducer interface {
	Input(msg *ProducerMessage)
	Close()
}

type ProducerMessage struct {
	DataID uint32
	Value  []byte
}

// Client gse data pipe client
type client struct {

	// The domain socket address
	endpoint string
	conn     net.Conn
	input    chan *ProducerMessage
	sigstop  chan bool
}

func (dhead *dataHead) packageData(data []byte) ([]byte, error) {

	buffer := new(bytes.Buffer)

	// package head
	if err := binary.Write(buffer, binary.BigEndian, dhead); nil != err {
		return nil, fmt.Errorf("can not package the data head,%v", err)
	}

	head := buffer.Bytes()
	head = append(head, data...)

	return head, nil
}

// Connect connect to gse data pipe
func (gsec *client) connect() error {

	conn, err := net.Dial("unix", gsec.endpoint)

	if err != nil {
		return fmt.Errorf("no gse data pipe  available, maybe gseagent is not running")
	}

	glog.V(3).Infof("current endpoint of gsedatapipe: %s", gsec.endpoint)
	gsec.conn = conn
	return nil
}

// Send write data to data pipe
func (gsec *client) send(dataid uint32, data []byte) error {

	if gsec.conn == nil {
		if error := gsec.connect(); error != nil {
			return error
		}
	}
	//glog.V(0).Info(string(data), len(data))
	dhead := dataHead{0xc01, dataid, 0, uint32(len(data)), [2]uint32{0, 0}}

	packageData, err := dhead.packageData(data)
	if nil != err {
		return err
	}

	err = gsec.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	if err != nil {
		glog.Errorf(" SET WRITE DEAD LINE FAILED: %v", err)
	}

	//glog.V(0).Info(string(packageData[24:]), dhead.bodylen)
	//var n int
	if _, err = gsec.conn.Write(packageData); err != nil {
		glog.Errorf("SEND DATA TO DATA PIPE FAILED: %v", err)
		gsec.conn.Close()
		gsec.conn = nil
		return err
	}

	//glog.V(0).Info("already send:%d", n)

	return nil
}

// Close close gse data pipe
func (gsec *client) close() error {

	gsec.sigstop <- true

	return nil
}

func (gsec *client) Close() {

	gsec.close()
}

func (gsec *client) Input(msg *ProducerMessage) {

	select {
	case gsec.input <- msg:
	default:
		glog.Errorf("pipe is full ")
	}
	return
}

// New create a new client of gse data pipe
func New(endpoint string) (AsyncProducer, error) {

	cli := &client{
		endpoint: endpoint,
		input:    make(chan *ProducerMessage, 4096),
		sigstop:  make(chan bool)}

	if err := cli.connect(); nil != err {

		glog.V(0).Infof("can not connect remote pipe : %s, error info: %v", cli.endpoint, err)
	}

	go func() {

		defer func() {

			cli.conn.Close()
			cli.conn = nil
		}()

		for {

			select {

			case msg := <-cli.input:

				if err := cli.send(msg.DataID, msg.Value); nil != err {

					glog.Errorf("can not send data to remote pipe : %s, error info: %v", cli.endpoint, err)

				}

			case <-cli.sigstop:

				//glog.Errorf("will disconnect with the remote pipe : %s,", cli.endpoint)
				//close(cli.input)
				//close(cli.sigstop)

				glog.Errorf("disconnected with the remote pipe : %s,", cli.endpoint)
				return
			}
		}

	}()

	return cli, nil
}
