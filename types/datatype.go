package types

import (
	"math"
)

// DataType is generic enumeration (enum type) which represents possible column
// data types.
type DataType string

const (
	Int    DataType = "int"
	Float  DataType = "float"
	Bool   DataType = "bool"
	String DataType = "string"
)

func IsType(v interface{}, t DataType) bool {

	switch v.(type) {
	case int, int8, int16, int32, int64:
		if t == Int {
			return true
		}
	case float32, float64:
		if t == Float {
			return true
		}
		if t == Int {
			// math.NaN() can be used as Int: IsType(math.NaN(), Int) --> true
			if v, ok := v.(float32); ok {
				if math.IsNaN(float64(v)) {
					return true
				}
			}
			if v, ok := v.(float64); ok {
				if math.IsNaN(v) {
					return true
				}
			}
		}
	case bool:
		if t == Bool {
			return true
		}
	case string:
		if t == String {
			return true
		}
	case DataType:
		if v == t {
			return true
		}
	}

	return false

}
