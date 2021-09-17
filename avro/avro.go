package avro

import (
	"fmt"

	"github.com/hamba/avro"
)

// Marshaller is an interface for marshalling to and from binary encoding bytes using avro
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
	avroSchema, err := avro.Parse(marshaller.Schema)
	if err != nil {
		return nil, fmt.Errorf("error parsing avro schema: %s", err)
	}

	data, err := avro.Marshal(avroSchema, model)
	if err != nil {
		return nil, fmt.Errorf("error marshalling model using Avro schema: %s", err)
	}

	return data, nil
}

func (marshaller *AvroMarshaller) Unmarshal(data []byte, model interface{}) error {
	avroSchema, err := avro.Parse(marshaller.Schema)
	if err != nil {
		return fmt.Errorf("error parsing avro schema: %s", err)
	}

	err = avro.Unmarshal(avroSchema, data, &model)
	if err != nil {
		return fmt.Errorf("error unmarshalling into model using Avro schema: %s", err)
	}

	return nil
}
