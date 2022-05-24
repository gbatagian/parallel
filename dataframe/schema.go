package dataframe

import (
	"parallel/types"
)

// Structure representing the schema definition fof a dataframe.
type Schema struct {
	Columns []Column
}

func evalSchemaForRow(r Row, s Schema) bool {

	for idx, value := range r.Values {
		if !(types.IsType(value, s.Columns[idx].Type)) {
			return false
		}
	}
	return true

}

func evalSchemaForRawData(d []interface{}, s Schema) bool {

	for idx, value := range d {
		if !(types.IsType(value, s.Columns[idx].Type)) {
			return false
		}
	}
	return true

}

func SchemaOK(i interface{}, s Schema) bool {

	// Evaluate schema for Row struct
	if r, ok := i.(Row); ok {
		return evalSchemaForRow(r, s)
	}

	// Evaluate schema for raw data
	if d, ok := i.([]interface{}); ok {
		return evalSchemaForRawData(d, s)
	}

	return false
}
