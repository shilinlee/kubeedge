package opengemini

import (
	"testing"
	"time"

	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/kubeedge/mapper-generator/pkg/common"
	"github.com/stretchr/testify/assert"
)

func Test_NewDbClient(t *testing.T) {
	conf := `{
			"username": admin,
		}`
	dbc, err := NewDatabaseClient([]byte(conf), nil)
	assert.EqualError(t, err, `invalid character 'a' looking for beginning of value`)

	conf = `{
			"url": "http://127.0.0.1:8086",
			"username": "admin",
			"password": "mock@pwd",
			"database": "db0",
			"retentionPolicy": "rp0"
		}`
	standard := `{
			"measurement": "mst",
			"tags": {
					"tag1": "val1",
					"tag2": "val2"
					},
			"fieldKey": "cpu"
			}`
	dbc, err = NewDatabaseClient([]byte(conf), []byte(standard))
	assert.NoError(t, err)
	_, err = dbc.InitDbClient()
	assert.NoError(t, err)
}

type MockOpenGeminiClient struct {
	client.Client
}

func (MockOpenGeminiClient) Write(bp client.BatchPoints) error {
	return nil
}

func (MockOpenGeminiClient) Close() error {
	return nil
}

func Test_AddData(t *testing.T) {
	conf := `{
			"url": "http://127.0.0.1:8086",
			"username": "admin",
			"password": "mock@pwd",
			"database": "db0",
			"retentionPolicy": "rp0"
		}`
	standard := `{
			"measurement": "mst",
			"tags": {
					"tag1": "val1",
					"tag2": "val2"
					},
			"fieldKey": "cpu"
			}`
	dbc, err := NewDatabaseClient([]byte(conf), []byte(standard))
	assert.NoError(t, err)

	cli := &MockOpenGeminiClient{}

	data := &common.DataModel{
		DeviceName:   "dev1",
		PropertyName: "property",
		Value:        "99",
		Type:         "",
		TimeStamp:    time.Now().UnixNano(),
	}
	err = dbc.AddData(data, cli)
	assert.NoError(t, err)
	err = dbc.CloseSession(cli)
	assert.NoError(t, err)
}
