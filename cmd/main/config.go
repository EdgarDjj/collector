package main

import (
	"github.com/spf13/viper"
)

type UDP struct {
	Host string
	Port int
}

type Kafka struct {
	Host string
	Port int
}

type Config struct {
	udpConfig *UDP
	kafkaConfig *Kafka
}

func InitConfigByViper() *viper.Viper {
	config := viper.New()
	config.AddConfigPath("$GOPATH/collector/config/")
	config.SetConfigName("configuration.yml")
	config.SetConfigType("yml")
	if err := config.ReadInConfig(); err != nil {
		panic(err)
	}
	return config
}

// 配置读取
func GetConfig() (c Config){
	var baseInfo = InitConfigByViper()
	c.udpConfig = &UDP {
		Host: baseInfo.GetString("udp.host"),
		Port: baseInfo.GetInt("udp.port"),
	}
	c.kafkaConfig = &Kafka {
		Host: baseInfo.GetString("kafka.host"),
		Port: baseInfo.GetInt("kafka.port"),
	}
	return c
}