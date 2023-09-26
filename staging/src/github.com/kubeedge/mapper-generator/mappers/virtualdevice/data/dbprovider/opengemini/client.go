package opengemini

import (
	"encoding/json"
	"time"

	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/kubeedge/mapper-generator/pkg/common"
	"k8s.io/klog/v2"
)

type DatabaseConfig struct {
	Config   *ConfigData   `json:"config,omitempty"`
	Standard *DataStandard `json:"standard,omitempty"`
}

type ConfigData struct {
	Url             string `json:"url,omitempty"`
	Username        string `json:"username,omitempty"`
	Password        string `json:"password,omitempty"`
	Database        string `json:"database,omitempty"`
	RetentionPolicy string `json:"retentionPolicy,omitempty"`
}

type DataStandard struct {
	Measurement string            `json:"measurement,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	FieldKey    string            `json:"fieldKey,omitempty"`
}

func NewDatabaseClient(config json.RawMessage, standard json.RawMessage) (*DatabaseConfig, error) {
	configData := new(ConfigData)
	dataStandard := new(DataStandard)
	err := json.Unmarshal(config, configData)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(standard, dataStandard)
	if err != nil {
		return nil, err
	}
	return &DatabaseConfig{
		Config:   configData,
		Standard: dataStandard,
	}, nil

}

func (d *DatabaseConfig) InitDbClient() (client.Client, error) {
	conf := client.HTTPConfig{
		Addr:     d.Config.Url,
		Username: d.Config.Username,
		Password: d.Config.Password,
	}
	return client.NewHTTPClient(conf)
}

func (d *DatabaseConfig) CloseSession(cli client.Client) error {
	return cli.Close()
}

func (d *DatabaseConfig) AddData(data *common.DataModel, cli client.Client) error {
	conf := client.BatchPointsConfig{
		Database:        d.Config.Database,
		RetentionPolicy: d.Config.RetentionPolicy,
	}
	bps, err := client.NewBatchPoints(conf)
	if err != nil {
		return err
	}
	point, err := client.NewPoint(
		d.Standard.Measurement,
		d.Standard.Tags,
		map[string]interface{}{d.Standard.FieldKey: data.Value},
		time.Now())
	if err != nil {
		return err
	}
	bps.AddPoint(point)
	// write point immediately
	err = cli.Write(bps)
	if err != nil {
		klog.V(4).Info("write point failed", err)
		return err
	}
	return nil
}
