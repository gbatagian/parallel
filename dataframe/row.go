package dataframe

import (
	"fmt"
	"parallel/types"
)

type Row struct {
	Values []interface{}
	Schema Schema
}

func (r *Row) createSchema() Schema {

	var s Schema

	for i, v := range r.Values {
		switch v.(type) {
		case int, int8, int16, int32, int64:
			s.Columns = append(s.Columns, Column{fmt.Sprintf("column_%d", i), types.Int})
		case float32, float64:
			s.Columns = append(s.Columns, Column{fmt.Sprintf("column_%d", i), types.Float})
		case bool:
			s.Columns = append(s.Columns, Column{fmt.Sprintf("column_%d", i), types.Bool})
		case string:
			s.Columns = append(s.Columns, Column{fmt.Sprintf("column_%d", i), types.String})
		}

	}

	return s
}

func (r *Row) createSchemaWithColumnNames(c []string) Schema {

	var s Schema

	for i, v := range r.Values {
		switch v.(type) {
		case int, int8, int16, int32, int64:
			s.Columns = append(s.Columns, Column{c[i], types.Int})
		case float32, float64:
			s.Columns = append(s.Columns, Column{c[i], types.Float})
		case bool:
			s.Columns = append(s.Columns, Column{c[i], types.Bool})
		case string:
			s.Columns = append(s.Columns, Column{c[i], types.String})
		}

	}

	return s

}

func createRowNoSchema(v []interface{}) Row {

	r := Row{Values: v}
	r.Schema = r.createSchema()

	return r
}

func createRowWithColumnNames(v []interface{}, c []string) Row {

	if len(c) != len(v) {
		msg := fmt.Sprintf(
			"Uneven sizes for values and columns:\n   - Values: %v (length %d)\n   - Schema: %v (length %d)",
			v, len(v),
			c, len(c),
		)
		panic(msg)
	}

	r := Row{Values: v}
	r.Schema = r.createSchemaWithColumnNames(c)

	return r
}

func createRowWithSchema(v []interface{}, s Schema) Row {

	if len(s.Columns) != len(v) {
		msg := fmt.Sprintf(
			"Uneven sizes for values and schema columns:\n   - Values: %v (length %d)\n   - Schema: %+v (length %d)",
			v, len(v),
			s, len(s.Columns),
		)
		panic(msg)
	}

	for i, value := range v {

		if !(types.IsType(value, s.Columns[i].Type)) {
			msg := fmt.Sprintf(
				"Invalid schema provided for row:\n   - Value %v in index position %d is of type %T\n   - Column in schema in position %d is of type %v",
				value, i, v[i],
				i, s.Columns[i].Type,
			)
			panic(msg)
		}
	}

	return Row{Values: v, Schema: s}

}

func CreateRow(v []interface{}, i ...interface{}) Row {

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
	return createRowWithSchema(v, input.(Schema))

}
