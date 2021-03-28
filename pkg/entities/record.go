package entities

import (
	"bytes"
	"collector/pkg/util"
	"encoding/binary"
	"fmt"
)
/*
该程序包包含记录中字段的编码。使用本地缓冲区在此处构建记录，然后再写入消息缓冲区。
相反，我们应该将字段直接写到消息上而不使用本地缓冲区吗？首先，我们将记录本地缓冲区。有一个接口并将功能公开给用户。
 */
type Record interface {
	PrepareRecord() (uint16, error)
	AddInfoElement(element *InfoElementWithValue, isDecoding bool) (uint16, error)
	// TODO: Functions for multiple elements as well.
	GetBuffer() *bytes.Buffer
	GetTemplateID() uint16
	GetFieldCount() uint16
	GetOrderedElementList() []*InfoElementWithValue
	GetInfoElementWithValue(name string) (*InfoElementWithValue, bool)
	GetMinDataRecordLen() uint16
}

type baseRecord struct {
	buff               bytes.Buffer
	len                uint16
	fieldCount         uint16
	templateID         uint16
	orderedElementList []*InfoElementWithValue
	elementsMap        map[string]*InfoElementWithValue
	Record
}

type dataRecord struct {
	*baseRecord
}

func NewDataRecord(id uint16) *dataRecord {
	return &dataRecord{
		&baseRecord{
			buff:               bytes.Buffer{},
			len:                0,
			fieldCount:         0,
			templateID:         id,
			orderedElementList: make([]*InfoElementWithValue, 0),
			elementsMap:        make(map[string]*InfoElementWithValue),
		},
	}
}

type templateRecord struct {
	*baseRecord
	// Minimum data record length required to be sent for this template.
	// Elements with variable length are considered to be one byte.
	minDataRecLength uint16
}

func NewTemplateRecord(count uint16, id uint16) *templateRecord {
	return &templateRecord{
		&baseRecord{
			buff:               bytes.Buffer{},
			len:                0,
			fieldCount:         count,
			templateID:         id,
			orderedElementList: make([]*InfoElementWithValue, 0),
			elementsMap:        make(map[string]*InfoElementWithValue),
		},
		0,
	}
}

func (b *baseRecord) GetBuffer() *bytes.Buffer {
	return &b.buff
}

func (b *baseRecord) GetTemplateID() uint16 {
	return b.templateID
}

func (b *baseRecord) GetFieldCount() uint16 {
	return b.fieldCount
}

func (d *baseRecord) GetOrderedElementList() []*InfoElementWithValue {
	return d.orderedElementList
}

func (b *baseRecord) GetInfoElementWithValue(name string) (*InfoElementWithValue, bool) {
	if element, exist := b.elementsMap[name]; exist {
		return element, exist
	} else {
		return nil, false
	}
}

func (d *dataRecord) PrepareRecord() (uint16, error) {
	// We do not have to do anything if it is data record
	return 0, nil
}

func (d *dataRecord) AddInfoElement(element *InfoElementWithValue, isDecoding bool) (uint16, error) {
	d.fieldCount++
	initialLength := d.buff.Len()
	var value interface{}
	var err error
	if isDecoding {
		value, err = DecodeToIEDataType(element.Element.DataType, element.Value)
	} else {
		value, err = EncodeToIEDataType(element.Element.DataType, element.Value, &d.buff)
	}

	if err != nil {
		return 0, err
	}
	ie := NewInfoElementWithValue(element.Element, value)
	d.orderedElementList = append(d.orderedElementList, ie)
	d.elementsMap[element.Element.Name] = ie
	if err != nil {
		return 0, err
	}
	return uint16(d.buff.Len() - initialLength), nil
}

func (t *templateRecord) PrepareRecord() (uint16, error) {
	// Add Template Record Header
	initialLength := t.buff.Len()
	err := util.Encode(&t.buff, binary.BigEndian, t.templateID, t.fieldCount)
	if err != nil {
		return 0, fmt.Errorf("AddInfoElement(templateRecord) error in writing template header: %v", err)
	}
	return uint16(t.buff.Len() - initialLength), nil
}

func (t *templateRecord) AddInfoElement(element *InfoElementWithValue, isDecoding bool) (uint16, error) {
	// val could be used to specify smaller length than default? For now assert it to be nil
	if element.Value != nil {
		return 0, fmt.Errorf("AddInfoElement(templateRecord) cannot take value %v (nil is expected)", element.Value)
	}
	initialLength := t.buff.Len()
	// Add field specifier {elementID: uint16, elementLen: uint16}
	err := util.Encode(&t.buff, binary.BigEndian, element.Element.ElementId, element.Element.Len)
	if err != nil {
		return 0, err
	}
	if element.Element.EnterpriseId != 0 {
		// Set the MSB of elementID to 1 as per RFC7011
		t.buff.Bytes()[initialLength] = t.buff.Bytes()[initialLength] | 0x80
		err = util.Encode(&t.buff, binary.BigEndian, element.Element.EnterpriseId)
		if err != nil {
			return 0, err
		}
	}
	t.orderedElementList = append(t.orderedElementList, element)
	t.elementsMap[element.Element.Name] = element
	// Keep track of minimum data record length required for sanity check
	if element.Element.Len == VariableLength {
		t.minDataRecLength = t.minDataRecLength + 1
	} else {
		t.minDataRecLength = t.minDataRecLength + element.Element.Len
	}
	return uint16(t.buff.Len() - initialLength), nil
}

func (t *templateRecord) GetMinDataRecordLen() uint16 {
	return t.minDataRecLength
}
