package avro

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var testSchema1 = `{ "type": "record",
 "name": "example1",
 "namespace": "correct",
 "fields": [
    {"name": "manager", "type": "string"},
    {"name": "team_name", "type": "string"},
    {"name": "ownerOfTeam", "type": "string"},
    {"name": "kind-of-sport", "type": "string"},
    {"name": "uri", "type": "string"},
    {"name": "has_changed_name", "type": "boolean"},
    {"name": "number_of_players", "type": "int"}
 ]
}`

var testSchema2 = `{ "type": "record",
 "name": "example2",
 "namespace": "incorrect",
 "fields": [
    {"name": "manager", "type": "string"},
    {"name": "team_name", "type": "string"},
    {"name": "owner", "type": "string"},
    {"name": "sport", "type": "string"},
    {"name": "goals", "type": "int"}
 ]
}`

var testSchema3 = `{ "type": "record",
 "name": "example",
 "namespace": "valid",
 "fields": [
      {"name": "manager", "type": "string"},
      {"name": "winning_years","type":["null",{"type":"array","items":"string"}]},
      {"name": "teamName", "type": "string"},
      {"name": "owner", "type": "string"},
      {"name": "sport", "type": "string"},
      {"name": "mascot", "type": "string"},
      {"name": "are_good", "type": "boolean"},
      {"name": "number_of_players", "type": "int"}
 ]
}`

var testNestedSchema = `
{
  "type": "record",
  "name": "transaction_closed",
  "namespace": "accounts",
  "fields": [
	{
            "name" : "transaction_url",
            "type" : "string"
	},
	{
            "name" : "presenter",
            "type" : {
                "type" : "record",
                "name" : "presenter_record",
                "fields" : [
                    {
                        "name" : "email",
                        "type" : "string"
                    }
                ]
            }
        }
    ]
}`

var testNestedArraySchema = `{
  "type": "record",
  "name": "filing_received",
  "namespace": "accounts",
  "fields": [
        {
            "name" : "application_id",
            "type" : "string"
        },
        {
            "name" : "channel_id",
            "type" : "string"
        },
        {
            "name" : "items",
            "type" : {
                "type" : "array",
                "items" : {
                    "name" : "transaction",
                    "type" : "record",
                    "fields" : [
                        {
                            "name" : "submission_id",
                            "type" : "string"
                        }
                    ]
                }
            }
        }
    ]
}
`

var testOptionalSchema = `{ 
 "type": "record",
 "name": "example1",
 "namespace": "correct",
 "fields": [
    {"name": "manager", "type": ["null", "string"]},
    {"name": "team_name", "type": "string"},
    {"name": "owner", "type": "string"},
    {"name": "sport", "type": "string"},
    {"name": "are_good", "type": "boolean"},
    {"name": "number_of_players", "type": "int"}
 ]
}`

type testData1 struct {
	Manager         string   `avro:"manager"`
	TeamName        string   `avro:"team_name"`
	Owner           string   `avro:"ownerOfTeam"`
	Sport           string   `avro:"kind-of-sport"`
	URI             string   `avro:"uri"`
	Players         []string `avro:"players"`
	PlayerOfTheYear string   `avro:"-" json:"player_of_the_year"`
	HasChangedName  bool     `avro:"has_changed_name"`
	NumberOfPlayers int32    `avro:"number_of_players"`
}

type testData2 struct {
	Manager  string `avro:"manager"`
	TeamName string `avro:"team_name"`
	Owner    string `avro:"owner"`
	Sport    string `avro:"sport"`
	Goals    int64  `avro:"goals"`
}

type testData3 struct {
	Manager         string   `avro:"manager"`
	TeamName        string   `avro:"teamName"`
	Owner           string   `avro:"owner"`
	Sport           string   `avro:"sport"`
	Mascot          string   `avro:"mascot"`
	WinningYears    []string `avro:"winning_years"`
	AreGood         bool     `avro:"are_good"`
	NumberOfPlayers int32    `avro:"number_of_players"`
}

type testData4 struct {
	Manager         string   `avro:"manager"`
	TeamName        string   `avro:"teamName"`
	Owner           string   `avro:"owner"`
	Sport           string   `avro:"sport"`
	Mascot          string   `avro:"-"`
	WinningYears    []string `avro:"winning_years"`
	AreGood         bool     `avro:"are_good"`
	NumberOfPlayers int32    `avro:"number_of_players"`
}

type testOptionalData struct {
	Manager         string `avro:"manager,omitempty"`
	TeamName        string `avro:"team_name,omitempty"`
	Owner           string `avro:"owner"`
	Sport           string `avro:"sport"`
	AreGood         bool   `avro:"are_good"`
	NumberOfPlayers int32  `avro:"number_of_players"`
}

type TestParent struct {
	TransactionURL string    `avro:"transaction_url"`
	Presenter      TestChild `avro:"presenter"`
}

type TestChild struct {
	Email string `avro:"email"`
}

type FilingReceived struct {
	ApplicationID string `avro:"application_id"`
	ChannelID     string `avro:"channel_id"`
	Items         []Item `avro:"items"`
}

type Item struct {
	SubmissionID string `avro:"submission_id"`
}

func TestUnitMarshal(t *testing.T) {
	Convey("Correctly marshal avro to byte array", t, func() {
		cs := &AvroMarshaller{
			Schema: testSchema1,
		}

		cd := &testData1{
			Manager:         "Pardew, Alan",
			TeamName:        "Crystal Palace FC",
			Owner:           "Long, Martin",
			Sport:           "Football",
			URI:             "/football/crystalpalace/123456789",
			PlayerOfTheYear: "Yannick Bolasie",
			HasChangedName:  false,
			Players:         []string{"Hugo Lloris", "Harry Kane"},
			NumberOfPlayers: int32(11),
		}

		bufferBytes1, err1 := cs.Marshal(cd)
		So(err1, ShouldBeNil)
		So(bufferBytes1, ShouldNotBeNil)
	})

	Convey("Marshal should return an error unless given a pointer to a struct", t, func() {
		cs := &AvroMarshaller{
			Schema: testSchema1,
		}

		test := "string"
		bufferBytes1b, err1b := cs.Marshal(test)
		So(err1b, ShouldNotBeNil)
		// So(err1b, ShouldHaveSameTypeAs, ErrUnsupportedType(reflect.ValueOf(test).Kind()))
		So(bufferBytes1b, ShouldBeNil)
	})

	Convey("Marshal should return an error if field is not of type string", t, func() {
		incs := &AvroMarshaller{
			Schema: testSchema2,
		}

		id := &testData2{
			Manager:  "Pardew, Alan",
			TeamName: "Crystal Palace FC",
			Owner:    "Long, Martin",
			Sport:    "Football",
			Goals:    int64(10),
		}

		bufferBytes2, err2 := incs.Marshal(id)
		So(err2, ShouldNotBeNil)
		// So(err2, ShouldEqual, ErrUnsupportedFieldType)
		So(bufferBytes2, ShouldBeNil)
	})

	Convey("Successfully marshal a nested schema", t, func() {
		nest := &AvroMarshaller{Schema: testNestedSchema}

		nestStruct := TestParent{
			TransactionURL: "/transaction/1389y4937493/accounts/68736438764/abridged",
			Presenter: TestChild{
				Email: "test1@wsdkjdb.com",
			},
		}

		bytes, err := nest.Marshal(nestStruct)
		So(bytes, ShouldNotBeEmpty)
		So(err, ShouldBeNil)
	})

	Convey("Successfully marshal a nested array schema", t, func() {
		nest := &AvroMarshaller{Schema: testNestedArraySchema}

		nestStruct := FilingReceived{
			ApplicationID: "1234",
			ChannelID:     "3456",
			Items: []Item{
				{
					SubmissionID: "5677",
				},
				{
					SubmissionID: "three",
				},
			},
		}

		bytes, err := nest.Marshal(nestStruct)
		So(err, ShouldBeNil)
		So(bytes, ShouldNotBeEmpty)
	})

	Convey("Correctly marshal avro to byte array with omitempty tag value", t, func() {
		cs := &AvroMarshaller{
			Schema: testOptionalSchema,
		}

		cd := &testOptionalData{
			Manager:         "",
			TeamName:        "Crystal Palace FC",
			Owner:           "Long, Martin",
			Sport:           "Football",
			AreGood:         false,
			NumberOfPlayers: int32(11),
		}

		bufferBytes1, err1 := cs.Marshal(cd)
		So(err1, ShouldBeNil)
		So(bufferBytes1, ShouldNotBeNil)
	})
}

func TestUnitUnmarshal(t *testing.T) {
	Convey("Correctly unmarshal byte array", t, func() {
		message, err := createMessage(testSchema3)
		So(err, ShouldBeNil)

		cs := &AvroMarshaller{
			Schema: testSchema3,
		}

		var data testData4

		err1 := cs.Unmarshal(message, &data)
		So(err1, ShouldBeNil)
		So(data.Manager, ShouldNotBeNil)
		So(data.Manager, ShouldEqual, "John Elway")
		So(data.TeamName, ShouldNotBeNil)
		So(data.TeamName, ShouldEqual, "Denver Broncos")
		So(data.Owner, ShouldNotBeNil)
		So(data.Owner, ShouldEqual, "Pat Bowlen")
		So(data.Sport, ShouldNotBeNil)
		So(data.Sport, ShouldEqual, "American Football")
		So(data.Mascot, ShouldNotBeNil)
		So(data.Mascot, ShouldEqual, "")
		So(data.AreGood, ShouldNotBeNil)
		So(data.AreGood, ShouldBeTrue)
		So(data.WinningYears, ShouldContain, "2017")
		So(data.WinningYears, ShouldContain, "1992")
	})

	Convey("Correctly unmarshal nil value", t, func() {
		message, err := createOptionalMessage(testOptionalSchema, "")
		So(err, ShouldBeNil)

		cs := &AvroMarshaller{
			Schema: testOptionalSchema,
		}

		var data testOptionalData

		err1 := cs.Unmarshal(message, &data)
		So(err1, ShouldBeNil)
		So(data.Manager, ShouldBeEmpty)
		So(data.TeamName, ShouldNotBeNil)
		So(data.TeamName, ShouldEqual, "Crystal Palace FC")
		So(data.Owner, ShouldNotBeNil)
		So(data.Owner, ShouldEqual, "Long, Martin")
		So(data.Sport, ShouldNotBeNil)
		So(data.Sport, ShouldEqual, "Football")
		So(data.AreGood, ShouldNotBeNil)
		So(data.AreGood, ShouldBeFalse)
		So(data.NumberOfPlayers, ShouldNotBeNil)
		So(data.NumberOfPlayers, ShouldEqual, 11)
	})

	Convey("Correctly unmarshal nested byte array", t, func() {
		message, err := createNestedMessage(testNestedSchema)
		So(err, ShouldBeNil)

		cs := &AvroMarshaller{
			Schema: testNestedSchema,
		}

		var data TestParent

		err = cs.Unmarshal(message, &data)
		So(err, ShouldBeNil)
		So(data.TransactionURL, ShouldNotBeEmpty)
		So(data.TransactionURL, ShouldEqual, "/1234/transactions")
		So(data.Presenter, ShouldNotBeEmpty)
		So(data.Presenter.Email, ShouldNotBeEmpty)
		So(data.Presenter.Email, ShouldEqual, "blargh@companieshouse.gov.uk")
	})

	Convey("Correctly unmarshal nested array schema", t, func() {
		message, err := createNestedArrayMessage(testNestedArraySchema)
		So(err, ShouldBeNil)

		cs := &AvroMarshaller{
			Schema: testNestedArraySchema,
		}

		var data FilingReceived

		err = cs.Unmarshal(message, &data)
		So(err, ShouldBeNil)
		So(data.ApplicationID, ShouldNotBeEmpty)
		So(data.ApplicationID, ShouldEqual, "1234")
		So(data.ChannelID, ShouldNotBeEmpty)
		So(data.ChannelID, ShouldEqual, "3456")
		So(data.Items, ShouldNotBeEmpty)
		So(data.Items[0].SubmissionID, ShouldNotBeEmpty)
		So(data.Items[0].SubmissionID, ShouldEqual, "5677")
		So(data.Items[1].SubmissionID, ShouldNotBeEmpty)
		So(data.Items[1].SubmissionID, ShouldEqual, "three")
	})

	Convey("Check error return for unsupported interface types", t, func() {
		message, err := createMessage(testSchema3)
		So(err, ShouldBeNil)

		cs := &AvroMarshaller{
			Schema: testSchema3,
		}

		data := ""
		// reflectData := reflect.ValueOf(data)

		err1 := cs.Unmarshal(message, data)
		So(err1, ShouldNotBeNil)
		// So(err1, ShouldResemble, ErrUnsupportedType(reflectData.Kind()))
	})
}

func createMessage(schema string) ([]byte, error) {
	marshalSchema := &AvroMarshaller{
		Schema: schema,
	}

	data := &testData3{
		Manager:         "John Elway",
		TeamName:        "Denver Broncos",
		Owner:           "Pat Bowlen",
		Sport:           "American Football",
		Mascot:          "Bear",
		WinningYears:    []string{"2017", "1992"},
		AreGood:         true,
		NumberOfPlayers: 11,
	}

	message, err := marshalSchema.Marshal(data)
	if err != nil {
		return nil, err
	}

	return message, nil
}

func createOptionalMessage(schema string, manager string) ([]byte, error) {
	marshalSchema := &AvroMarshaller{
		Schema: schema,
	}

	cd := &testOptionalData{
		Manager:         manager,
		TeamName:        "Crystal Palace FC",
		Owner:           "Long, Martin",
		Sport:           "Football",
		AreGood:         false,
		NumberOfPlayers: int32(11),
	}

	message, err := marshalSchema.Marshal(cd)
	if err != nil {
		return nil, err
	}

	return message, nil
}

func createNestedMessage(schema string) ([]byte, error) {
	marshalSchema := &AvroMarshaller{
		Schema: schema,
	}

	data := &TestParent{
		TransactionURL: "/1234/transactions",
		Presenter: TestChild{
			Email: "blargh@companieshouse.gov.uk",
		},
	}

	message, err := marshalSchema.Marshal(data)
	if err != nil {
		return nil, err
	}

	return message, nil
}

func createNestedArrayMessage(schema string) ([]byte, error) {
	marshalSchema := &AvroMarshaller{
		Schema: schema,
	}

	data := FilingReceived{
		ApplicationID: "1234",
		ChannelID:     "3456",
		Items: []Item{
			{
				SubmissionID: "5677",
			},
			{
				SubmissionID: "three",
			},
		},
	}

	message, err := marshalSchema.Marshal(data)
	if err != nil {
		return nil, err
	}

	return message, nil
}
