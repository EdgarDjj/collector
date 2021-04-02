package entities

import (
	"bytes"
	"encoding/binary"
)

// IPFIX message.
type Message struct {
	buffer        *bytes.Buffer
	version       uint16
	length        uint16
	seqNumber     uint32
	obsDomainID   uint32
	exportTime    uint32
	exportAddress string
	isDecoding    bool
	set           Set
}

func NewMessage(isDecoding bool) *Message {
	return &Message{
		buffer:     &bytes.Buffer{},
		isDecoding: isDecoding,
	}
}

func (m *Message) GetVersion() uint16 {
	return m.version
}

func (m *Message) SetVersion(version uint16) {
	m.version = version
	if !m.isDecoding {
		binary.BigEndian.PutUint16(m.buffer.Bytes()[0:2], version)
	}
}

func (m *Message) GetMessageLen() uint16 {
	return m.length
}

func (m *Message) SetMessageLen(len uint16) {
	m.length = len
	if !m.isDecoding {
		binary.BigEndian.PutUint16(m.buffer.Bytes()[2:4], len)
	}
}

func (m *Message) GetSequenceNum() uint32 {
	return m.seqNumber
}

func (m *Message) SetSequenceNum(seqNum uint32) {
	m.seqNumber = seqNum
	if !m.isDecoding {
		binary.BigEndian.PutUint32(m.buffer.Bytes()[8:12], seqNum)
	}
}

func (m *Message) GetObsDomainID() uint32 {
	return m.obsDomainID
}

func (m *Message) SetObsDomainID(obsDomainID uint32) {
	m.obsDomainID = obsDomainID
	if !m.isDecoding {
		binary.BigEndian.PutUint32(m.buffer.Bytes()[12:], obsDomainID)
	}
}

func (m *Message) GetExportTime() uint32 {
	return m.exportTime
}

func (m *Message) SetExportTime(exportTime uint32) {
	m.exportTime = exportTime
	if !m.isDecoding {
		binary.BigEndian.PutUint32(m.buffer.Bytes()[4:8], exportTime)
	}
}

func (m *Message) GetExportAddress() string {
	return m.exportAddress
}

func (m *Message) SetExportAddress(ipAddr string) {
	m.exportAddress = ipAddr
}

func (m *Message) GetSet() Set {
	return m.set
}

func (m *Message) AddSet(set Set) {
	m.set = set
}

func (m *Message) GetMsgBuffer() *bytes.Buffer {
	return m.buffer
}

func (m *Message) GetMsgBufferLen() int {
	return m.buffer.Len()
}

func (m *Message) WriteToMsgBuffer(bytesToWrite []byte) (int, error) {
	return m.buffer.Write(bytesToWrite)
}

func (m *Message) CreateHeader() (int, error) {
	header := make([]byte, 16)
	return m.WriteToMsgBuffer(header)
}

func (m *Message) ResetMsgBuffer() {
	m.buffer.Reset()
}
