// Package avro provides a user functionality to return the
// avro encoding of s.
package avro

import (
	"bytes"
	"fmt"

	"github.com/elodina/go-avro"
)

// Marshaller is an interface for marshalling to and from binary using avro
type Marshaller interface {
	// Marshal is a function to encode a model into binary data using avro
	Marshal(model interface{}) ([]byte, error)

	// Unmarshal is a function to decode binary data into a model using Avro
	Unmarshal(message []byte, model interface{}) error
}

type AvroMarshaller struct {
	Schema string
}

func (marshaller *AvroMarshaller) Marshal(model interface{}) ([]byte, error) {
	avroSchema, err := avro.ParseSchema(marshaller.Schema)
	if err != nil {
		return nil, fmt.Errorf("error parsing avro schema: %s", err)
	}

	writer := avro.NewSpecificDatumWriter()
	writer.SetSchema(avroSchema)

	buffer := new(bytes.Buffer)
	encoder := avro.NewBinaryEncoder(buffer)

	err = writer.Write(model, encoder)
	if err != nil {
		return nil, fmt.Errorf("error writing avro schema: %s", err)
	}

	return buffer.Bytes(), nil
}

func (marshaller *AvroMarshaller) Unmarshal(data []byte, model interface{}) error {
	avroSchema, err := avro.ParseSchema(marshaller.Schema)
	if err != nil {
		return fmt.Errorf("error parsing avro schema: %s", err)
	}

	reader := avro.NewSpecificDatumReader()
	reader.SetSchema(avroSchema)

	decoder := avro.NewBinaryDecoder(data)

	return reader.Read(model, decoder)
}
