package row

import (
	"fmt"
	"math"
	"parallel/column"
	"parallel/schema"
	"parallel/types"
)

type Row struct {
	Values []interface{}
	Schema schema.Schema
}

func (r1 *Row) Equals(r2 Row) bool {
	if !r1.Schema.Equals(r2.Schema) {
		return false
	}
	for idx, v := range r1.Values {
		if v != r2.Values[idx] {
			return false
		}
	}
	return true
}

func (r1 *Row) EqualValues(r2 Row) bool {
	if len(r1.Values) != len(r2.Values) {
		return false
	}
	for idx, v1 := range r1.Values {
		v2 := r2.Values[idx]
		if v1 != v2 {
			v1, _ := v1.(float64)
			v2, _ := v2.(float64)
			if !(math.IsNaN(v1) && math.IsNaN(v2)) {
				return false
			}
		}
	}
	return true
}

func (r *Row) SchemaOK(s schema.Schema) bool {

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

func (r *Row) Slice(sIdx int, eIdx int) Row {

	nVal := len(r.Values)
	if eIdx > nVal {
		eIdx = nVal
	}

	if sIdx > eIdx {
		panic(fmt.Sprintf("Invadid row slice indexes, start: %d, end: %d", sIdx, eIdx))
	}

	if sIdx == eIdx {
		return Row{}
	}

	newRow := Row{}
	newRow.Schema.Columns = r.Schema.Columns[sIdx:eIdx]
	newRow.Values = r.Values[sIdx:eIdx]

	return newRow

}

func (r *Row) createSchema() schema.Schema {

	var s schema.Schema

	for i, v := range r.Values {
		switch v.(type) {
		case int, int8, int16, int32, int64:
			s.Columns = append(s.Columns, column.Column{Name: fmt.Sprintf("column_%d", i), Type: types.Int})
		case float32, float64:
			s.Columns = append(s.Columns, column.Column{Name: fmt.Sprintf("column_%d", i), Type: types.Float})
		case bool:
			s.Columns = append(s.Columns, column.Column{Name: fmt.Sprintf("column_%d", i), Type: types.Bool})
		case string:
			s.Columns = append(s.Columns, column.Column{Name: fmt.Sprintf("column_%d", i), Type: types.String})
		}

	}

	return s
}

func (r *Row) createSchemaWithColumnNames(c []string) schema.Schema {

	var s schema.Schema

	for i, v := range r.Values {
		switch v.(type) {
		case int, int8, int16, int32, int64:
			s.Columns = append(s.Columns, column.Column{Name: c[i], Type: types.Int})
		case float32, float64:
			s.Columns = append(s.Columns, column.Column{Name: c[i], Type: types.Float})
		case bool:
			s.Columns = append(s.Columns, column.Column{Name: c[i], Type: types.Bool})
		case string:
			s.Columns = append(s.Columns, column.Column{Name: c[i], Type: types.String})
		}

	}

	return s

}

func createRowNoSchema(v *[]interface{}) Row {

	r := Row{Values: *v}
	r.Schema = r.createSchema()

	return r
}

func createRowWithColumnNames(v *[]interface{}, c []string) Row {

	if len(c) != len(*v) {
		msg := fmt.Sprintf(
			"Uneven sizes for values and columns:\n   - Values: %v (length %d)\n   - Schema: %v (length %d)",
			v, len(*v),
			c, len(c),
		)
		panic(msg)
	}

	r := Row{Values: *v}
	r.Schema = r.createSchemaWithColumnNames(c)

	return r
}

func createRowWithSchema(v *[]interface{}, s schema.Schema) Row {

	if len(s.Columns) != len(*v) {
		msg := fmt.Sprintf(
			"Uneven sizes for values and schema columns:\n   - Values: %v (length %d)\n   - Schema: %+v (length %d)",
			v, len(*v),
			s, len(s.Columns),
		)
		panic(msg)
	}

	for i, value := range *v {

		if !(types.IsType(value, s.Columns[i].Type)) {
			msg := fmt.Sprintf(
				"Invalid schema provided for row:\n   - Value %v in index position %d is of type %T\n   - Column in schema in position %d is of type %v",
				value, i, (*v)[i],
				i, s.Columns[i].Type,
			)
			panic(msg)
		}
	}

	return Row{Values: *v, Schema: s}

}

func CreateRow(v *[]interface{}, i ...interface{}) Row {

	// Case1: Schema was not provided in row definition.
	if len(i) == 0 {
		return createRowNoSchema(v)
	}

	input := i[0] // Variadic was used to make i optional, when not missing always evaluated in the 0 index.

	// Case 2: Array of strings with column names was provided in row definition.
	if _, ok := input.([]string); ok {
		return createRowWithColumnNames(v, input.([]string))
	}

	// Case 3: Schema was provided in row definition.
	return createRowWithSchema(v, input.(schema.Schema))

}
