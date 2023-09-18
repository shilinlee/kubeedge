package driver

import (
	"k8s.io/klog/v2"
	"math/rand"
	"sync"
)

func NewClient(commonProtocol ProtocolCommonConfig,
	protocol ProtocolConfig) (*CustomizedClient, error) {
	client := &CustomizedClient{
		ProtocolCommonConfig: commonProtocol,
		ProtocolConfig:       protocol,
		deviceMutex:          sync.Mutex{},
		// TODO initialize the variables you added
	}
	return client, nil
}

func (c *CustomizedClient) InitDevice() error {
	// TODO: add init operation
	// you can use c.ProtocolConfig and c.ProtocolCommonConfig
	klog.V(1).Info("*****Initialize the device*******")
	klog.V(1).Infof("SlaveID:%v", c.ProtocolConfig.SlaveID)
	klog.V(1).Infof("SerialPort:%v", c.ProtocolCommonConfig.SerialPort)
	klog.V(1).Infof("CommonCustom:%v", c.ProtocolCommonConfig.CommonCustomizedValues.SerialType)
	return nil
}

func (c *CustomizedClient) GetDeviceData(visitor *VisitorConfig) (interface{}, error) {
	// TODO: get true device's data
	// you can use c.ProtocolConfig,c.ProtocolCommonConfig and visitor
	klog.V(1).Info("*******collect device *********")
	klog.V(1).Infof("+++++visitor.name = %s", visitor.Name)
	if visitor.Name == "temperature" {
		return rand.Intn(40), nil
	} else if visitor.Name == "temperature-enable" {
		return visitor.Offset, nil
	}
	return nil, nil
}

func (c *CustomizedClient) SetDeviceData(data interface{}, visitor *VisitorConfig) error {
	// TODO: set device's data
	// you can use c.ProtocolConfig,c.ProtocolCommonConfig and visitor
	klog.V(1).Info("***** Set the device data*******")
	klog.V(1).Infof("Register:%v", visitor.Register)
	klog.V(1).Infof("IsRegisterSwap:%v", visitor.IsRegisterSwap)
	klog.V(1).Infof("IsSwap:%v", visitor.IsSwap)
	return nil
}

func (c *CustomizedClient) StopDevice() error {
	// TODO: stop device
	// you can use c.ProtocolConfig and c.ProtocolCommonConfig
	klog.V(1).Info("Stop Modbus Device")
	return nil
}
