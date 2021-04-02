package main

import (
	"github.com/spf13/viper"
	"log"
	"path/filepath"
	"strconv"
	"strings"
)

const IPFIXTransport = "udp"

type UDP struct {
	Host string
	Port int
}

type Kafka struct {
	Host       string
	Port       uint16
	Topic      string
	BrokerList []string
}

type Config struct {
	udpConfig   *UDP
	kafkaConfig *Kafka
	maxBufSize  int
	ttl         int
	host        string
}

func InitConfigByViper() *viper.Viper {
	config := viper.New()
	step := string(filepath.Separator)
	path := "$GOPATH" + step + "collector" + step
	log.Println("Config Path load in :" + path)
	config.AddConfigPath(path)
	config.SetConfigName("configuration.yml")
	config.SetConfigType("yml")
	if err := config.ReadInConfig(); err != nil {
		panic(err)
	}
	return config
}

func GetConfig() (c Config) {
	var baseInfo = InitConfigByViper()
	c.udpConfig = &UDP{
		Host: baseInfo.GetString("udp.host"),
		Port: baseInfo.GetInt("udp.port"),
	}
	c.kafkaConfig = &Kafka{
		Host:  baseInfo.GetString("kafka.host"),
		Port:  uint16(baseInfo.GetInt("kafka.port")),
		Topic: baseInfo.GetString("kafka.topic"),
	}
	c.host = strings.Join([]string{baseInfo.GetString(c.kafkaConfig.Host), strconv.Itoa(int(c.kafkaConfig.Port))}, ":")
	c.kafkaConfig.BrokerList = []string{c.host}
	c.maxBufSize = baseInfo.GetInt("maxBufSize")
	c.ttl = baseInfo.GetInt("ttl")
	return c
}
