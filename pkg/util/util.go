package util

import (
	"bytes"
	"collector/pkg/entities"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// Encode writes the multiple inputs of different possible datatypes to the io writer.
func Encode(buff io.Writer, byteOrder binary.ByteOrder, inputs ...interface{}) error {
	var err error
	for _, in := range inputs {
		err = binary.Write(buff, byteOrder, in)
		if err != nil {
			return fmt.Errorf("error in encoding data %v: %v", in, err)
		}
	}
	return nil
}

// Decode decodes data from io reader to specified interfaces
/* Example:
var num1 uint16
var num2 uint32
// read the buffer 2 bytes and 4 bytes sequentially
// decode and output corresponding uint16 and uint32 number into num1 and num2 respectively
err := Decode(buffer, &num1, &num2)
*/
func Decode(buffer io.Reader, byteOrder binary.ByteOrder, outputs ...interface{}) error {
	var err error
	for _, out := range outputs {
		err = binary.Read(buffer, byteOrder, out)
		if err != nil {
			return fmt.Errorf("error in decoding data: %v", err)
		}
	}
	return nil
}

func PrintIPFIXMessage(msg *entities.Message) bytes.Buffer {
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
	return buf
}
