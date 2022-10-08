package schema

import (
	"parallel/column"
	"parallel/types"
)

type Schema struct {
	Columns []column.Column
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

func (s *Schema) ColumnIndex(columnName string) int {
	for idx, c := range s.Columns {
		if c.Name == columnName {
			return idx
		}
	}
	return -1
}

func SchemaOKForRawData(d []interface{}, s Schema) bool {

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
