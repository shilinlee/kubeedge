package driver

import (
	"sync"

	"github.com/kubeedge/mapper-generator/pkg/common"
)

// CustomizedDev is the customized device configuration and client information.
type CustomizedDev struct {
	Instance         common.DeviceInstance
	CustomizedClient *CustomizedClient
}

type CustomizedClient struct {
	// TODO add some variables to help you better implement device drivers
	intMaxValue int
	deviceMutex sync.Mutex
	ProtocolCommonConfig
	ProtocolConfig
}

type ProtocolConfig struct { //customizedprotocol字段
	ProtocolName       string `json:"protocolName"`
	ProtocolConfigData `json:"configData"`
}

type ProtocolConfigData struct {
	// TODO: add your config data according to configmap
	DeviceID int `json:"deviceID,omitempty"`
}

type ProtocolCommonConfig struct { //common字段
	// TODO: add your Common data according to configmap
	Com                    `json:"com"`
	CommonCustomizedValues `json:"customizedValues"`
}

type Com struct {
	SerialPort string `json:"serialPort"`
	DataBits   int    `json:"dataBits"`
	BaudRate   int    `json:"baudRate"`
	Parity     string `json:"parity"`
	StopBits   int    `json:"stopBits"`
}

type CommonCustomizedValues struct {
	// TODO: add your CommonCustomizedValues according to configmap
	ProtocolID int `json:"protocolID"`
}
type VisitorConfig struct {
	ProtocolName string `json:"protocolName"`
	//DataType     string `json:"dataType"`
	VisitorConfigData `json:"configData"`
}

type VisitorConfigData struct {
	// TODO: add your Visitor ConfigData according to configmap
	DataType string `json:"dataType"`
}
