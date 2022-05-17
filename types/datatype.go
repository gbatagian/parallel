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
