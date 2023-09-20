package dtcommon

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"k8s.io/klog/v2"

	"github.com/kubeedge/kubeedge/cloud/pkg/devicecontroller/constants"
	"github.com/kubeedge/kubeedge/pkg/apis/devices/v1alpha2"
	pb "github.com/kubeedge/kubeedge/pkg/apis/dmi/v1alpha1"
)

// ValidateValue validate value type
func ValidateValue(valueType string, value string) error {
	switch valueType {
	case "":
		valueType = constants.DataTypeString
		return nil
	case constants.DataTypeString:
		return nil
	case constants.DataTypeInt, constants.DataTypeInteger:
		_, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return errors.New("the value is not int or integer")
		}
		return nil
	case constants.DataTypeFloat:
		_, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.New("the value is not float")
		}
		return nil
	case constants.DataTypeBoolean:
		if strings.Compare(value, "true") != 0 && strings.Compare(value, "false") != 0 {
			return errors.New("the bool value must be true or false")
		}
		return nil
	case TypeDeleted:
		return nil
	default:
		return errors.New("the value type is not allowed")
	}
}

// ValidateTwinKey validate twin key
func ValidateTwinKey(key string) bool {
	pattern := "^[a-zA-Z0-9-_.,:/@#]{1,128}$"
	match, _ := regexp.MatchString(pattern, key)
	return match
}

// ValidateTwinValue validate twin value
func ValidateTwinValue(value string) bool {
	pattern := "^[a-zA-Z0-9-_.,:/@#]{1,512}$"
	match, _ := regexp.MatchString(pattern, value)
	return match
}

func GetProtocolNameOfDevice(device *v1alpha2.Device) (string, error) {
	protocol := device.Spec.Protocol
	if protocol.OpcUA != nil {
		return constants.OPCUA, nil
	}
	if protocol.Modbus != nil {
		return constants.Modbus, nil
	}
	if protocol.Bluetooth != nil {
		return constants.Bluetooth, nil
	}
	if protocol.CustomizedProtocol != nil {
		return protocol.CustomizedProtocol.ProtocolName, nil
	}
	return "", fmt.Errorf("cannot find protocol name for device %s", device.Name)
}
func ConvertDevice(device *v1alpha2.Device) (*pb.Device, error) {
	data, err := json.Marshal(device)
	if err != nil {
		klog.Errorf("fail to marshal device %s with err: %v", device.Name, err)
		return nil, err
	}

	var edgeDevice pb.Device
	err = json.Unmarshal(data, &edgeDevice)
	if err != nil {
		klog.Errorf("fail to unmarshal device %s with err: %v", device.Name, err)
		return nil, err
	}
	if device.Spec.Protocol.CustomizedProtocol != nil {
		// interface data to anypb.Any data
		configAnyData := make(map[string]*anypb.Any)
		for k, v := range device.Spec.Protocol.CustomizedProtocol.ConfigData.Data {
			anyValue, err := dataToAny(v)
			if err != nil {
				return nil, err
			}
			configAnyData[k] = anyValue
		}
		edgeDevice.Spec.Protocol.CustomizedProtocol.ConfigData.Data = configAnyData
	}
	if device.Spec.Protocol.Common.CustomizedValues != nil {
		// interface data to anypb.Any data
		configAnyData := make(map[string]*anypb.Any)
		for k, v := range device.Spec.Protocol.Common.CustomizedValues.Data {
			anyValue, err := dataToAny(v)
			if err != nil {
				return nil, err
			}
			configAnyData[k] = anyValue
		}
		edgeDevice.Spec.Protocol.Common.CustomizedValues.Data = configAnyData
	}
	var edgePropertyVisitors []*pb.DevicePropertyVisitor
	for i := range device.Spec.PropertyVisitors {
		var item *pb.DevicePropertyVisitor = new(pb.DevicePropertyVisitor)
		propertyData, err := json.Marshal(device.Spec.PropertyVisitors[i])
		if err != nil {
			klog.Errorf("fail to marshal device %s with err: %v", device.Name, err)
			return nil, err
		}
		err = json.Unmarshal(propertyData, item)
		if err != nil {
			klog.Errorf("fail to unmarshal device %s with err: %v", device.Name, err)
			return nil, err
		}
		if device.Spec.PropertyVisitors[i].CustomizedValues != nil {
			configAnyData := make(map[string]*anypb.Any)
			for k, v := range device.Spec.PropertyVisitors[i].CustomizedValues.Data {
				anyValue, err := dataToAny(v)
				if err != nil {
					return nil, err
				}
				configAnyData[k] = anyValue
			}
			item.CustomizedValues.Data = configAnyData
		}
		if device.Spec.PropertyVisitors[i].CustomizedProtocol != nil {
			configAnyData := make(map[string]*anypb.Any)
			for k, v := range device.Spec.PropertyVisitors[i].CustomizedProtocol.ConfigData.Data {
				anyValue, err := dataToAny(v)
				if err != nil {
					return nil, err
				}
				configAnyData[k] = anyValue
			}
			item.CustomizedProtocol.ConfigData.Data = configAnyData
		}
		edgePropertyVisitors = append(edgePropertyVisitors, item)
	}
	edgeDevice.Spec.PropertyVisitors = edgePropertyVisitors

	edgeDevice.Name = device.Name
	edgeDevice.Spec.DeviceModelReference = device.Spec.DeviceModelRef.Name

	return &edgeDevice, nil
}

func dataToAny(v interface{}) (*anypb.Any, error) {
	switch v.(type) {
	case string:
		strWrapper := wrapperspb.String(v.(string))
		anyStr, err := anypb.New(strWrapper)
		if err != nil {
			klog.Errorf("anypb new error: %v", err)
			return nil, err
		}
		return anyStr, nil
	case int8:
		intWrapper := wrapperspb.Int32(int32(v.(int8)))
		anyInt, err := anypb.New(intWrapper)
		if err != nil {
			klog.Errorf("anypb new error: %v", err)
			return nil, err
		}
		return anyInt, nil
	case int16:
		intWrapper := wrapperspb.Int32(int32(v.(int16)))
		anyInt, err := anypb.New(intWrapper)
		if err != nil {
			klog.Errorf("anypb new error: %v", err)
			return nil, err
		}
		return anyInt, nil
	case int32:
		intWrapper := wrapperspb.Int32(v.(int32))
		anyInt, err := anypb.New(intWrapper)
		if err != nil {
			klog.Errorf("anypb new error: %v", err)
			return nil, err
		}
		return anyInt, nil
	case int64:
		intWrapper := wrapperspb.Int64(v.(int64))
		anyInt, err := anypb.New(intWrapper)
		if err != nil {
			klog.Errorf("anypb new error: %v", err)
			return nil, err
		}
		return anyInt, nil
	case int:
		intWrapper := wrapperspb.Int32(int32(v.(int)))
		anyInt, err := anypb.New(intWrapper)
		if err != nil {
			klog.Errorf("anypb new error: %v", err)
			return nil, err
		}
		return anyInt, nil
	case float64:
		floatWrapper := wrapperspb.Float(float32(v.(float64)))
		anyFloat, err := anypb.New(floatWrapper)
		if err != nil {
			klog.Errorf("anypb new error: %v", err)
			return nil, err
		}
		return anyFloat, nil
	case float32:
		floatWrapper := wrapperspb.Float(float32(v.(float32)))
		anyFloat, err := anypb.New(floatWrapper)
		if err != nil {
			klog.Errorf("anypb new error: %v", err)
			return nil, err
		}
		return anyFloat, nil
	case bool:
		boolWrapper := wrapperspb.Bool(v.(bool))
		anyBool, err := anypb.New(boolWrapper)
		if err != nil {
			klog.Errorf("anypb new error: %v", err)
			return nil, err
		}
		return anyBool, nil
	default:
		return nil, fmt.Errorf("%v does not support converting to any", reflect.TypeOf(v))
	}
}

func ConvertDeviceModel(model *v1alpha2.DeviceModel) (*pb.DeviceModel, error) {
	data, err := json.Marshal(model)
	if err != nil {
		klog.Errorf("fail to marshal device model %s with err: %v", model.Name, err)
		return nil, err
	}

	var edgeDeviceModel pb.DeviceModel
	err = json.Unmarshal(data, &edgeDeviceModel)
	if err != nil {
		klog.Errorf("fail to unmarshal device model %s with err: %v", model.Name, err)
		return nil, err
	}
	edgeDeviceModel.Name = model.Name

	return &edgeDeviceModel, nil
}
