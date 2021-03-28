package entities

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	// TemplateRefreshTimeOut is the template refresh time out for exporting process
	TemplateRefreshTimeOut uint32 = 60
	// TemplateTTL is the template time to live for collecting process
	TemplateTTL = TemplateRefreshTimeOut * 3
	// TemplateSetID is the setID for template record
	TemplateSetID uint16 = 2
)

type ContentType uint8

const (
	Template ContentType = iota
	Data
	// Add OptionsTemplate too when it is supported
	Undefined = 255
)

type Set interface {
	PrepareSet(setType ContentType, templateID uint16) error
	ResetSet()
	GetBuffer() *bytes.Buffer
	GetSetType() ContentType
	UpdateLenInHeader()
	AddRecord(elements []*InfoElementWithValue, templateID uint16) error
	GetRecords() []Record
	GetNumberOfRecords() uint32
}

type set struct {
	// Pointer to message buffer
	buffer     *bytes.Buffer
	setType    ContentType
	records    []Record
	isDecoding bool
}

func NewSet(isDecoding bool) Set {
	return &set{
		buffer:     &bytes.Buffer{},
		records:    make([]Record, 0),
		isDecoding: isDecoding,
	}
}

func (s *set) PrepareSet(setType ContentType, templateID uint16) error {
	if setType == Undefined {
		return fmt.Errorf("set type is not properly defined")
	} else {
		s.setType = setType
	}
	if !s.isDecoding {
		// Create the set header and append it when encoding
		return s.createHeader(s.setType, templateID)
	}
	return nil
}

func (s *set) ResetSet() {
	s.buffer.Reset()
	s.setType = Undefined
	s.records = s.records[:0]
}

func (s *set) GetBuffer() *bytes.Buffer {
	return s.buffer
}

func (s *set) GetSetType() ContentType {
	return s.setType
}

func (s *set) UpdateLenInHeader() {
	// TODO:Add padding to the length when multiple sets are sent in IPFIX message
	if !s.isDecoding {
		// Add length to the set header
		binary.BigEndian.PutUint16(s.buffer.Bytes()[2:4], uint16(s.buffer.Len()))
	}
}

func (s *set) AddRecord(elements []*InfoElementWithValue, templateID uint16) error {
	var record Record
	if s.setType == Data {
		record = NewDataRecord(templateID)
	} else if s.setType == Template {
		record = NewTemplateRecord(uint16(len(elements)), templateID)
	} else {
		return fmt.Errorf("set type is not supported")
	}
	record.PrepareRecord()
	for _, element := range elements {
		record.AddInfoElement(element, s.isDecoding)
	}
	s.records = append(s.records, record)
	// write record to set when encoding
	if !s.isDecoding {
		recordBytes := record.GetBuffer().Bytes()
		bytesWritten, err := s.buffer.Write(recordBytes)
		if err != nil {
			return fmt.Errorf("error in writing the buffer to set: %v", err)
		}
		if bytesWritten != len(recordBytes) {
			return fmt.Errorf("bytes written length is not expected")
		}
	}
	return nil
}

func (s *set) GetRecords() []Record {
	return s.records
}

func (s *set) GetNumberOfRecords() uint32 {
	return uint32(len(s.records))
}

func (s *set) createHeader(setType ContentType, templateID uint16) error {
	header := make([]byte, 4)
	if setType == Template {
		binary.BigEndian.PutUint16(header[0:2], TemplateSetID)
	} else if setType == Data {
		binary.BigEndian.PutUint16(header[0:2], templateID)
	}
	if _, err := s.buffer.Write(header); err != nil {
		return err
	}
	return nil
}
