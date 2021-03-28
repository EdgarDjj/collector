package main

import (
	"collector/pkg/entities"
	"collector/pkg/protobuf"
	"encoding/binary"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"net"
)

var (
	KafkaConfigVersion sarama.KafkaVersion
)

type KafkaProducer struct {
	producer sarama.AsyncProducer
	topic    string
}

func NewKafkaProducer(asyncProducer sarama.AsyncProducer, topic string) *KafkaProducer {
	return &KafkaProducer{
		producer: asyncProducer,
		topic:    topic,
	}
}

// Publish takes in a message channel as input and converts all the messages on
// the message channel to flow messages in proto schema. This function exits when
// the input message channel is closed.
func (kp *KafkaProducer) Publish(msgCh chan *entities.Message) {
	for msg := range msgCh {
		flowMsgs := convertIPFIXMsgToFlowMsgs(msg)
		for _, flowMsg := range flowMsgs {
			kp.SendFlowMessage(flowMsg, true)
		}
	}
}

// SendFlowMessage takes in the flow message in proto schema, encodes it and sends
// it to on the producer channel. If kafkaDelimitMsgWithLen is set to true, it will
// return  a length-prefixed encoded message.
func (kp *KafkaProducer) SendFlowMessage(msg *protobuf.FlowMessage, kafkaDelimitMsgWithLen bool) {
	bytes, err := proto.Marshal(msg)
	if err != nil {
		fmt.Printf("Error when encoding flow message: %v\n", err)
		return
	}
	if kafkaDelimitMsgWithLen {
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(len(bytes)))
		bytes = append(b, bytes...)
	}

	kp.producer.Input() <- &sarama.ProducerMessage{
		Topic: kp.topic,
		Value: sarama.ByteEncoder(bytes),
	}
}

// convertIPFIXMsgToFlowMsgs converts data records in IPFIX message to flow messages
// in given proto schema.
func convertIPFIXMsgToFlowMsgs(msg *entities.Message) []*protobuf.FlowMessage {
	set := msg.GetSet()
	if set.GetSetType() == entities.Template {
		return nil
	}

	flowMsgs := make([]*protobuf.FlowMessage, 0)
	for _, record := range set.GetRecords() {
		flowMsg := &protobuf.FlowMessage{}
		flowMsg.TimeReceived = msg.GetExportTime()
		flowMsg.SequenceNumber = msg.GetSequenceNum()
		flowMsg.ObsDomainID = msg.GetObsDomainID()
		flowMsg.ExportAddress = msg.GetExportAddress()
		for _, ie := range record.GetOrderedElementList() {
			switch ie.Element.Name {
			case "flowStartSeconds":
				flowMsg.TimeFlowStartInSecs = ie.Value.(uint32)
			case "flowEndSeconds":
				flowMsg.TimeFlowEndInSecs = ie.Value.(uint32)
			case "sourceIPv4Address", "sourceIPv6Address":
				if flowMsg.SrcIP != "" {
					fmt.Printf("Do not expect source IP: %v to be filled already\n", flowMsg.SrcIP)
				}
				flowMsg.SrcIP = ie.Value.(net.IP).String()
			case "destinationIPv4Address", "destinationIPv6Address":
				if flowMsg.DstIP != "" {
					fmt.Printf("Do not expect destination IP: %v to be filled already\n", flowMsg.DstIP)
				}
				flowMsg.DstIP = ie.Value.(net.IP).String()
			case "sourceTransportPort":
				flowMsg.SrcPort = uint32(ie.Value.(uint16))
			case "destinationTransportPort":
				flowMsg.DstPort = uint32(ie.Value.(uint16))
			case "protocolIdentifier":
				flowMsg.Proto = uint32(ie.Value.(uint8))
			case "packetTotalCount":
				flowMsg.PacketsTotal = ie.Value.(uint64)
			case "octetTotalCount":
				flowMsg.BytesTotal = ie.Value.(uint64)
			case "packetDeltaCount":
				flowMsg.PacketsDelta = ie.Value.(uint64)
			case "octetDeltaCount":
				flowMsg.BytesDelta = ie.Value.(uint64)
			case "reversePacketTotalCount":
				flowMsg.ReversePacketsTotal = ie.Value.(uint64)
			case "reverseOctetTotalCount":
				flowMsg.ReverseBytesTotal = ie.Value.(uint64)
			case "reversePacketDeltaCount":
				flowMsg.ReversePacketsDelta = ie.Value.(uint64)
			case "reverseOctetDeltaCount":
				flowMsg.ReverseBytesDelta = ie.Value.(uint64)
			case "sourcePodNamespace":
				flowMsg.SrcPodNamespace = ie.Value.(string)
			case "sourcePodName":
				flowMsg.SrcPodName = ie.Value.(string)
			case "sourceNodeName":
				flowMsg.SrcNodeName = ie.Value.(string)
			case "destinationPodNamespace":
				flowMsg.DstPodNamespace = ie.Value.(string)
			case "destinationPodName":
				flowMsg.DstPodName = ie.Value.(string)
			case "destinationNodeName":
				flowMsg.DstNodeName = ie.Value.(string)
			case "destinationClusterIPv4", "destinationClusterIPv6":
				if flowMsg.DstClusterIP != "" {
					fmt.Printf("Do not expect destination cluster IP: %v to be filled already", flowMsg.DstClusterIP)
				}
				flowMsg.DstClusterIP = ie.Value.(net.IP).String()
			case "destinationServicePort":
				flowMsg.DstServicePort = uint32(ie.Value.(uint16))
			case "destinationServicePortName":
				flowMsg.DstServicePortName = ie.Value.(string)
			case "ingressNetworkPolicyName":
				flowMsg.IngressPolicyName = ie.Value.(string)
			case "ingressNetworkPolicyNamespace":
				flowMsg.IngressPolicyNamespace = ie.Value.(string)
			case "egressNetworkPolicyName":
				flowMsg.EgressPolicyName = ie.Value.(string)
			case "egressNetworkPolicyNamespace":
				flowMsg.EgressPolicyNamespace = ie.Value.(string)
			default:
				fmt.Printf("There is no field with name: %v in flow message (.proto schema)", ie.Element.Name)
			}
		}
		flowMsgs = append(flowMsgs, flowMsg)
	}
	return flowMsgs
}
