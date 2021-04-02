package main

import (
	"bytes"
	"github.com/Shopify/sarama"
	"log"
)

type KafkaProducer struct {
	producer sarama.AsyncProducer
	topic    string
}

func (kafkaProducer *KafkaProducer) SendMessageToKafka(buf bytes.Buffer) {
	// TODO： transport type marshal msg to byte

	log.Println("$$$ start send Message to Kafka")
	kafkaProducer.producer.Input() <- &sarama.ProducerMessage{
		Topic: kafkaProducer.topic,
		Value: sarama.ByteEncoder(buf.Bytes()),
	}
}

func InitSyncProducer(addrs []string, topic string, logErrors bool) (*KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = false

	asyncProducer, err := sarama.NewAsyncProducer(addrs, config)
	if err != nil {
		return nil, err
	}
	producer := newKafkaProducer(asyncProducer, topic)

	if logErrors {
		go func() {
			for msg := range asyncProducer.Errors() {
				log.Fatalln(msg)
			}
		}()
	}
	return producer, nil
}

func newKafkaProducer(asyncProducer sarama.AsyncProducer, topic string) *KafkaProducer {
	return &KafkaProducer{
		producer: asyncProducer,
		topic:    topic,
	}
}
