package main

import (
	"collector/pkg/entities"
	"collector/pkg/registry"
	"collector/pkg/util"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var c = GetConfig()

func main() {
	if err := run(); err != nil {
		log.Fatalf("Error when running IPFIX collector: %v", err)
	}
}

func run() error {
	log.Println("Starting IPFIX collector")
	// load the IPFIX global registry
	registry.LoadRegistry()
	cpInput := CollectorInput{
		Address:       c.host,
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

	collectProcess, err := InitCollectingProcess(cpInput)
	if err != nil {
		log.Fatalln("Failed to start Collecting Process", err)
		return err
	}

	kafkaProducer, err := InitSyncProducer(c.kafkaConfig.BrokerList, c.kafkaConfig.Topic, true)
	if err != nil {
		log.Fatalln("Failed to start Kafka producer", err)
		return err
	}

	messageReceived := make(chan *entities.Message)
	go func() {
		go collectProcess.Start()
		msgChan := collectProcess.GetMsgChan()
		for message := range msgChan {
			log.Println("Processing IPFIX message sucessfully")
			messageReceived <- message
		}
	}()
	stopCh := make(chan struct{})
	go signalHandler(stopCh, messageReceived, kafkaProducer)
	<-stopCh
	collectProcess.Stop()
	log.Println("Stopping IPFIX collector!")
	return nil
}

func signalHandler(stopCh chan struct{}, messageReceived chan *entities.Message, kafkaProducer *KafkaProducer) {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		select {
		case msg := <-messageReceived:
			// TODO: msg convert into kafka
			buf := util.PrintIPFIXMessage(msg)
			kafkaProducer.SendMessageToKafka(buf)
		case <-signalCh:
			close(stopCh)
			return
		}
	}
}
