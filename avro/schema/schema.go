//Package schema allows avro schema retrieval from schema registry
package schema

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
)

//ErrInvalidResponse is returned when schema registry returns an invalid response
var ErrInvalidResponse = errors.New("Invalid response from schema registry")

//Get returns a schema retrieved from the schema registry
func Get(url, name string) (string, error) {
	resp, err := http.Get(url + "/subjects/" + name + "/versions/latest")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != 200 {
		return "", ErrInvalidResponse
	}

	var sch map[string]interface{}
	if err := json.Unmarshal(body, &sch); err != nil {
		return "", err
	}

	var schema string
	var ok bool
	if schema, ok = sch["schema"].(string); !ok {
		return "", ErrInvalidResponse
	}

	return schema, nil
}
