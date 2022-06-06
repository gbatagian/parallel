package dataframe

import (
	"parallel/types"
)

// Structure representing the schema definition fof a dataframe.
type Schema struct {
	Columns []Column
}

func (s1 *Schema) Equals(s2 Schema) bool {
	if len(s1.Columns) != len(s2.Columns) {
		return false
	}
	for idx, c := range s1.Columns {
		if !c.Equals(s2.Columns[idx]) {
			return false
		}
	}
	return true
}

func evalSchemaForRow(r Row, s Schema) bool {

	if len(s.Columns) != len(r.Values) {
		return false
	}

	for idx, value := range r.Values {
		if !(types.IsType(value, s.Columns[idx].Type)) {
			return false
		}
	}

	return true

}

func evalSchemaForRawData(d []interface{}, s Schema) bool {

	if len(s.Columns) != len(d) {
		return false
	}

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
