package util

import (
	"encoding/binary"
	"fmt"
	"io"
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
