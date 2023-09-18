package tdengine

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/kubeedge/mapper-generator/pkg/common"
	_ "github.com/taosdata/driver-go/v3/taosRestful"
	"k8s.io/klog/v2"
	"strings"
	"time"
)

var (
	DB *sql.DB
)

type DataBaseConfig struct {
	Config *ConfigData `json:"config,omitempty"`
}
type ConfigData struct {
	Dsn string `json:"dsn,omitempty"`
}

func NewDataBaseClient(config json.RawMessage) (*DataBaseConfig, error) {
	configdata := new(ConfigData)
	err := json.Unmarshal(config, configdata)
	if err != nil {
		return nil, err
	}
	return &DataBaseConfig{
		Config: configdata,
	}, nil
}
func (d *DataBaseConfig) InitDbClient() error {
	var err error
	DB, err = sql.Open("taosRestful", d.Config.Dsn)
	if err != nil {
		klog.Errorf("init TDEngine db fail, err= %v:", err)
		//fmt.Printf("failed connect to TDengine:%v", err)
	} else {
		klog.V(1).Infof("init TDEngine database successfully")
	}
	return nil
}
func (d *DataBaseConfig) CloseSessio() {
	err := DB.Close()
	if err != nil {
		klog.V(4).Info("close  TDEngine failed")
	}
}
func (d *DataBaseConfig) AddData(data *common.DataModel) error {

	legal_table := strings.Replace(data.DeviceName, "-", "_", -1)
	legal_tag := strings.Replace(data.PropertyName, "-", "_", -1)

	stable_name := fmt.Sprintf("SHOW STABLES LIKE '%s'", legal_table)
	stabel := fmt.Sprintf("CREATE STABLE %s (ts timestamp, devicename binary(64), propertyname binary(64), data binary(64),type binary(64)) TAGS (localtion binary(64));", legal_table)
	datatime := time.Unix(data.TimeStamp/1e3, 0).Format("2006-01-02 15:04:05")
	insertSQL := fmt.Sprintf("INSERT INTO %s USING %s TAGS ('%s') VALUES('%v','%s', '%s', '%s', '%s');",
		legal_tag, legal_table, legal_tag, datatime, data.DeviceName, data.PropertyName, data.Value, data.Type)

	rows, _ := DB.Query(stable_name)

	if err := rows.Err(); err != nil {
		fmt.Printf("query stable failed：%v", err)
	}

	switch rows.Next() {
	case false:
		_, err := DB.Exec(stabel)
		if err != nil {
			klog.V(4).Infof("create stable failed %v\n", err)
		}
		_, err = DB.Exec(insertSQL)
		if err != nil {
			klog.Infof("failed add data to TdEngine:%v", err)
		}
	case true:
		_, err := DB.Exec(insertSQL)
		if err != nil {
			klog.Infof("failed add data to TdEngine:%v", err)
		}
	default:
		klog.Infoln("failed add data to TdEngine")
	}

	return nil
}
func (d *DataBaseConfig) GetDataByDeviceName(deviceName string) ([]*common.DataModel, error) {
	query := fmt.Sprintf("SELECT ts, devicename, propertyname, data, type FROM %s", deviceName)
	rows, err := DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []*common.DataModel
	for rows.Next() {
		var result common.DataModel
		var ts time.Time
		err := rows.Scan(&ts, &result.DeviceName, &result.PropertyName, &result.Value, &result.Type)
		if err != nil {
			//klog.Infof("scan error:\n", err)
			fmt.Printf("scan error:\n", err)
			return nil, err
		}
		result.TimeStamp = ts.Unix()
		results = append(results, &result)
	}
	return results, nil
}
func (d *DataBaseConfig) GetPropertyDataByDeviceName(deviceName string, propertyData string) ([]*common.DataModel, error) {
	//TODO implement me
	panic("implement me")
}
func (d *DataBaseConfig) GetDataByTimeRange(start int64, end int64) ([]*common.DataModel, error) {
	//TODO implement me
	panic("implement me")
}
func (d *DataBaseConfig) DeleteDataByTimeRange(start int64, end int64) ([]*common.DataModel, error) {
	//TODO implement me
	panic("implement me")
}
