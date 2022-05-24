package types

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
	case bool:
		if t == Bool {
			return true
		}
	case string:
		if t == String {
			return true
		}
	}

	return false

}
