package redis

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"github.com/kubeedge/mapper-generator/pkg/common"
	"k8s.io/klog/v2"
	"strconv"
)

type DataBaseConfig struct {
	Config *ConfigData
}

type ConfigData struct {
	Addr         string `json:"addr,omitempty"`
	Password     string `json:"password,omitempty"`
	DB           int64  `json:"db,omitempty"`
	PoolSize     int64  `json:"poolSize,omitempty"`
	MinIdleConns int64  `json:"minIdleConns,omitempty"`
}

func NewDataBaseClient(config json.RawMessage) (*DataBaseConfig, error) {
	configdata := new(ConfigData)
	err := json.Unmarshal(config, configdata)
	if err != nil {
		return nil, err
	}
	return &DataBaseConfig{Config: configdata}, nil
}

func (d *DataBaseConfig) InitDbClient() (*redis.Client, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:         d.Config.Addr,
		Password:     d.Config.Password,
		DB:           int(d.Config.DB),
		PoolSize:     int(d.Config.PoolSize),
		MinIdleConns: int(d.Config.MinIdleConns),
	})
	pong, err := redisClient.Ping(context.Background()).Result()
	if err != nil {
		klog.Errorf("init redis database failed, err = %v", err)
		return nil, err
	} else {
		klog.V(1).Infof("init redis database successfully, with return cmd %s", pong)
	}
	return redisClient, nil
}

func (d *DataBaseConfig) CloseSession(client *redis.Client) {
	err := client.Close()
	if err != nil {
		klog.V(4).Info("close database failed")
	}
}

func (d *DataBaseConfig) AddData(data *common.DataModel, client *redis.Client) error {
	ctx := context.Background()
	// The key to construct the ordered set, here DeviceName is used as the key
	klog.V(1).Infof("deviceName:%s", data.DeviceName)
	// Check if the current ordered set exists
	exists, err := client.Exists(ctx, data.DeviceName).Result()
	if err != nil {
		klog.V(4).Info("Exit AddData")
		return err
	}
	deviceData := "TimeStamp: " + strconv.FormatInt(data.TimeStamp, 10) + " PropertyName: " + data.PropertyName + " data: " + data.Value
	if exists == 0 {
		// The ordered set does not exist, create a new ordered set and add data
		_, err = client.ZAdd(ctx, data.DeviceName, &redis.Z{
			Score:  float64(data.TimeStamp),
			Member: deviceData,
		}).Result()
		if err != nil {
			klog.V(4).Info("Exit AddData")
			return err
		}
	} else {
		// The ordered set already exists, add data directly
		_, err = client.ZAdd(ctx, data.DeviceName, &redis.Z{
			Score:  float64(data.TimeStamp),
			Member: deviceData,
		}).Result()
		if err != nil {
			klog.V(4).Info("Exit AddData")
			return err
		}
	}
	return nil
}

//func (d *DataBaseConfig) GetDataByDeviceName(deviceName string) ([]*common.DataModel, error) {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (d *DataBaseConfig) GetPropertyDataByDeviceName(deviceName string, propertyData string) ([]*common.DataModel, error) {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (d *DataBaseConfig) GetDataByTimeRange(start int64, end int64) ([]*common.DataModel, error) {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (d *DataBaseConfig) DeleteDataByTimeRange(start int64, end int64) ([]*common.DataModel, error) {
//	//TODO implement me
//	panic("implement me")
//}
