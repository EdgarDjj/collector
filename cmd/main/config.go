package main

import (
	"fmt"
	"github.com/spf13/viper"
	"path/filepath"
)

type UDP struct {
	Host string
	Port int
}

type Kafka struct {
	Host  string
	Port  int
	Topic string
}

type Config struct {
	udpConfig   *UDP
	kafkaConfig *Kafka
	maxBufSize  int
	ttl         int
}

func InitConfigByViper() *viper.Viper {
	config := viper.New()
	step := string(filepath.Separator)
	path := "$GOPATH" + step + "collector" + step
	fmt.Println("Config Path load in :" + path)
	config.AddConfigPath(path)
	config.SetConfigName("configuration.yml")
	config.SetConfigType("yml")
	if err := config.ReadInConfig(); err != nil {
		panic(err)
	}
	return config
}

// 配置读取
func GetConfig() (c Config) {
	var baseInfo = InitConfigByViper()
	c.udpConfig = &UDP{
		Host: baseInfo.GetString("udp.host"),
		Port: baseInfo.GetInt("udp.port"),
	}
	c.kafkaConfig = &Kafka{
		Host:  baseInfo.GetString("kafka.host"),
		Port:  baseInfo.GetInt("kafka.port"),
		Topic: baseInfo.GetString("kafka.topic"),
	}
	c.maxBufSize = baseInfo.GetInt("maxBufSize")
	c.ttl = baseInfo.GetInt("ttl")
	return c
}
