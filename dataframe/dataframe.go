package dataframe

import (
	"fmt"
	"math"
	"parallel/types"
)

type Dataframe struct {
	Rows   []Row
	Schema Schema
}

func (df1 *Dataframe) Equals(df2 Dataframe) bool {
	if df1.IsEmpty() && df2.IsEmpty() {
		return true
	}
	if !df1.Schema.Equals(df2.Schema) {
		return false
	}
	for idx, r := range df1.Rows {
		if !r.EqualValues(df2.Rows[idx]) {
			return false
		}
	}
	return true
}

func (df *Dataframe) IsEmpty() bool {

	if df.Schema.Equals(Schema{}) && len(df.Rows) == 0 {
		// This is the case: df = Dataframe{}
		return true
	}

	if df.Schema.Equals(Schema{}) && len(df.Rows) == 1 && df.Rows[0].Equals(Row{}) {
		/* This is the case when df is created from an empty slice of values,
		e.g.

		rawValues := [][]interface{}{
		 	{},
		}

		df := dataframe.CreateDataframe(rawValues)
		*/
		return true
	}

	return false

}

func (df *Dataframe) ColumnNames() []string {
	var n []string

	for _, c := range df.Schema.Columns {
		n = append(n, c.Name)
	}
	return n
}

func (df *Dataframe) ColumnTypes() []types.DataType {
	var t []types.DataType

	for _, c := range df.Schema.Columns {
		t = append(t, c.Type)
	}

	return t
}

func (df *Dataframe) updateDataframeSchema(r Row) {

	dfSchemaLen := len(df.Schema.Columns)
	rowSchemaLen := len(r.Schema.Columns)

	if dfSchemaLen < rowSchemaLen {
		// row has a larger schema (more values than the df columnd)
		df.updateDfSchemaFromRowWithmLargerSchema(r)
	} else {
		// row has a smaller schema (less values than the df columnd)
		df.applyDfSchemaInRowWithSmallerSchema(&r)

	}

	df.updateColumnsTypesBasedOnSchema(r.Schema)

}

func (df *Dataframe) updateColumnsTypesBasedOnSchema(s Schema) {
	for idx, c := range s.Columns {
		if !(types.IsType(df.Schema.Columns[idx].Type, c.Type)) {
			df.updateColumnTypeInPosition(idx, c.Type)
		}
	}
}

func (df *Dataframe) updateDfSchemaFromRowWithmLargerSchema(r Row) {

	// Extend df schema with the extra columns present in row
	dfSchemaLen := len(df.Schema.Columns)
	df.Schema.Columns = append(df.Schema.Columns, r.Schema.Columns[dfSchemaLen:]...)
	var dummyValues []interface{}

	// Create dummy values to populate the extra columns on the previous rows
	for idx, c := range r.Schema.Columns[dfSchemaLen:] {
		dfColumnIdx := idx + dfSchemaLen

		if types.IsType(c.Type, types.Int) {
			dummyValues = append(dummyValues, math.NaN())
		} else if types.IsType(c.Type, types.Float) {
			dummyValues = append(dummyValues, math.NaN())
		} else if types.IsType(c.Type, types.String) {
			dummyValues = append(dummyValues, "")
		} else if types.IsType(c.Type, types.Bool) {
			dummyValues = append(dummyValues, "")
			df.Schema.Columns[dfColumnIdx].Type = types.String // because "" was used as dummy value for the missing bool value
		}
	}

	// Populate previous rows with dummy values
	for idx, r := range df.Rows[:len(df.Rows)-1] {
		df.Rows[idx].Values = append(r.Values, dummyValues...)
	}

}

func (df *Dataframe) applyDfSchemaInRowWithSmallerSchema(r *Row) {

	// Add dummy values to the row in order to complu with the df larger schema
	rowLen := len(r.Schema.Columns)
	var dummyValues []interface{}

	for _, c := range df.Schema.Columns[rowLen:] {

		if types.IsType(c.Type, types.Int) {
			dummyValues = append(dummyValues, math.NaN())
			r.Schema.Columns = append(r.Schema.Columns, Column{Name: "", Type: types.Int})
		} else if types.IsType(c.Type, types.Float) {
			dummyValues = append(dummyValues, math.NaN())
			r.Schema.Columns = append(r.Schema.Columns, Column{Name: "", Type: types.Float})
		} else if types.IsType(c.Type, types.String) {
			dummyValues = append(dummyValues, "")
			r.Schema.Columns = append(r.Schema.Columns, Column{Name: "", Type: types.String})
		} else if types.IsType(c.Type, types.Bool) {
			dummyValues = append(dummyValues, "")
			r.Schema.Columns = append(r.Schema.Columns, Column{Name: "", Type: types.String}) // because "" was used as dummy value for the missing bool value
		}
	}

	df.Rows[len(df.Rows)-1].Values = append(df.Rows[len(df.Rows)-1].Values, dummyValues...)

}

func (df *Dataframe) updateColumnTypeInPosition(idx int, t types.DataType) {

	switch df.Schema.Columns[idx].Type {
	case types.Int:
		switch t {
		case types.Float:
			df.Schema.Columns[idx].Type = types.Float
			df.updateValuesFormatInPosition(idx, types.Float)
		default:
			df.Schema.Columns[idx].Type = types.String
			df.updateValuesFormatInPosition(idx, types.String)
		}
	case types.Float:
		switch t {
		case types.Int:
			df.Schema.Columns[idx].Type = types.Float
			df.updateValuesFormatInPosition(idx, types.Float)
		default:
			df.Schema.Columns[idx].Type = types.String
			df.updateValuesFormatInPosition(idx, types.String)
		}
	default:
		df.Schema.Columns[idx].Type = types.String
		df.updateValuesFormatInPosition(idx, types.String)
	}

}

func (df *Dataframe) updateValuesFormatInPosition(idx int, f types.DataType) {

	switch f {
	case types.Int:
		for _, r := range df.Rows {
			if !types.IsType(r.Values[idx], f) {
				if v, ok := r.Values[idx].(float64); ok {
					r.Values[idx] = int(v)
				} else if v, ok := r.Values[idx].(float32); ok {
					r.Values[idx] = int(v)
				}
			}
		}
	case types.Float:
		for _, r := range df.Rows {
			if !types.IsType(r.Values[idx], f) {
				if v, ok := r.Values[idx].(int); ok {
					r.Values[idx] = float64(v)
				} else if v, ok := r.Values[idx].(int8); ok {
					r.Values[idx] = float64(v)
				} else if v, ok := r.Values[idx].(int16); ok {
					r.Values[idx] = float64(v)
				} else if v, ok := r.Values[idx].(int32); ok {
					r.Values[idx] = float64(v)
				} else if v, ok := r.Values[idx].(int64); ok {
					r.Values[idx] = float64(v)
				}
			}
		}
	default:
		for _, r := range df.Rows {
			if !types.IsType(r.Values[idx], f) {
				r.Values[idx] = fmt.Sprintf("%v", r.Values[idx])
			}
		}
	}
}

func createDataframeWithNoSchemaInfo(rows [][]interface{}) Dataframe {

	df := Dataframe{}

	for _, r := range rows {

		row := CreateRow(r)
		schema := row.Schema
		df.Rows = append(df.Rows, row)

		if len(df.Schema.Columns) == 0 {
			df.Schema = schema
		} // Initialise schema

		if !SchemaOK(row, df.Schema) {
			df.updateDataframeSchema(row)
		} // Update schema - if needed

	}

	return df

}

func createDataframeWithColumnNames(rows [][]interface{}, c []string) Dataframe {

	df := Dataframe{}

	for _, r := range rows {
		row := CreateRow(r, c)
		schema := row.Schema
		df.Rows = append(df.Rows, row)
		if len(df.Schema.Columns) == 0 {
			df.Schema = schema
		} // Initialise schema

		if !SchemaOK(row, df.Schema) {
			df.updateDataframeSchema(row)
		} // Update schema - if needed
	}

	return df

}

func createDataframeWithSchema(rows [][]interface{}, s Schema) Dataframe {

	df := Dataframe{}
	df.Schema = s

	for _, r := range rows {
		row := CreateRow(r, s)
		df.Rows = append(df.Rows, row)
	}

	return df
}

func CreateDataframe(rows [][]interface{}, i ...interface{}) Dataframe {

	// Case1: No schema related information was provided in dataframe definition.
	if len(i) == 0 {
		return createDataframeWithNoSchemaInfo(rows)
	}

	input := i[0] // Variadic was used to make i optional, when not missing always evaluated in the 0 index.

	// Case 2: The provided info is the column names.
	if columnNames, ok := input.([]string); ok {
		return createDataframeWithColumnNames(rows, columnNames)
	}

	// Case 3: Schema was provided
	return createDataframeWithSchema(rows, input.(Schema))
}
