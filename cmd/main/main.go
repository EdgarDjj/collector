package main

import (
	"bytes"
	"collector/pkg/entities"
	"collector/pkg/registry"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)


func printIPFIXMessage(msg *entities.Message) {
	var buf bytes.Buffer
	fmt.Fprint(&buf, "\nIPFIX-HDR:\n")
	fmt.Fprintf(&buf, "  version: %v,  Message Length: %v\n", msg.GetVersion(), msg.GetMessageLen())
	fmt.Fprintf(&buf, "  Exported Time: %v (%v)\n", msg.GetExportTime(), time.Unix(int64(msg.GetExportTime()), 0))
	fmt.Fprintf(&buf, "  Sequence No.: %v,  Observation Domain ID: %v\n", msg.GetSequenceNum(), msg.GetObsDomainID())

	set := msg.GetSet()
	if set.GetSetType() == entities.Template {
		fmt.Fprint(&buf, "TEMPLATE SET:\n")
		for i, record := range set.GetRecords() {
			fmt.Fprintf(&buf, "  TEMPLATE RECORD-%d:\n", i)
			for _, ie := range record.GetOrderedElementList() {
				fmt.Fprintf(&buf, "    %s: len=%d (enterprise ID = %d) \n", ie.Element.Name, ie.Element.Len, ie.Element.EnterpriseId)
			}
		}
	} else {
		fmt.Fprint(&buf, "DATA SET:\n")
		for i, record := range set.GetRecords() {
			fmt.Fprintf(&buf, "  DATA RECORD-%d:\n", i)
			for _, ie := range record.GetOrderedElementList() {
				fmt.Fprintf(&buf, "    %s: %v \n", ie.Element.Name, ie.Value)
			}
		}
	}
}

var (
	IPFIXAddr      string
	IPFIXPort      uint16
	IPFIXTransport string
	LOGGER log.Logger
)


func main() {
	// load the IPFIX global registry
	registry.LoadRegistry()
	// load config
	c := GetConfig()
	IPFIXAddr = c.udpConfig.Host
	IPFIXTransport = "udp"
	IPFIXPort = uint16(c.udpConfig.Port)

	cpInput := CollectorInput{
		Address:       IPFIXAddr + ":" + strconv.Itoa(int(IPFIXPort)),
		Protocol:      IPFIXTransport,
		MaxBufferSize: 65535,
		TemplateTTL:   0,
		IsEncrypted:   false,
		ServerCert:    nil,
		ServerKey:     nil,
	}

	cp, err := InitCollectingProcess(cpInput)
	if err != nil {
		panic(err)
	}
	// 监听连接与接受消息
	messageReceived := make(chan *entities.Message)
	go func() {
		go cp.Start()
		msgChan := cp.GetMsgChan()
		for message := range msgChan {
			LOGGER.Print("$$ Processing IPFIX message!!!!!!")
			messageReceived <- message
		}
	}()

	stopCh := make(chan struct{})
	go signalHandler(stopCh, messageReceived)
	<-stopCh
	cp.Stop()
}

func signalHandler(stopCh chan struct{}, messageReceived chan *entities.Message) {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		select {
		case msg := <-messageReceived:
			printIPFIXMessage(msg)
		case <-signalCh:
			close(stopCh)
			return
		}
	}
}
