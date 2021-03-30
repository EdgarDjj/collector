package main

import (
	"bytes"
	"collector/pkg/entities"
	"collector/pkg/registry"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var (
	IPFIXAddr      string
	IPFIXPort      uint16
	IPFIXTransport string
)
var c = GetConfig()

func main() {
	// load the IPFIX global registry
	registry.LoadRegistry()
	// load config
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
	if c.maxBufSize != 0 {
		cpInput.MaxBufferSize = uint16(c.maxBufSize)
	}
	if c.ttl != 0 {
		cpInput.TemplateTTL = uint32(c.ttl)
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
			// kafka的消费
			buf := printIPFIXMessage(msg)
			sendMessageByKafka(buf)
		case <-signalCh:
			close(stopCh)
			return
		}
	}
}

// 三种连接kafka方式 1、无认证 2、TLS认证 3、SASL/PLAIN
func sendMessageByKafka(buf bytes.Buffer) {
	topic := c.kafkaConfig.Topic
	producer, err := newSyncProducer()
	log.Printf("$$$ start send Message to Kafka, the topic is %s\n", topic)
	if err != nil {
		log.Fatalln("Failed to start start Sarama producer: ", err)
		return
	}
	msgToSend := buf.Bytes()
	messageSend := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msgToSend),
	}
	pid, offset, err := producer.SendMessage(messageSend)
	if err != nil {
		log.Fatalln("send message Failed!")
	} else {
		log.Printf("Message send: %s pid: %v offset: %v", messageSend, pid, offset)
	}
}

func newSyncProducer() (sarama.SyncProducer, error) {
	kafkaHost := c.kafkaConfig.Host
	kafkaPort := c.kafkaConfig.Port
	addr := strings.Join([]string{kafkaHost, strconv.Itoa(kafkaPort)}, ":")
	brokerList := []string{addr}
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		return nil, err
	}
	return producer, err
}

func printIPFIXMessage(msg *entities.Message) bytes.Buffer {
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
	fmt.Println(buf.String())
	fmt.Println("$$$ Data:", buf.Bytes())
	return buf
}
