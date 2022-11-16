package dataframe

import (
	"fmt"
	"math"
	"parallel/column"
	"parallel/row"
	"parallel/schema"
	"parallel/types"
)

type Dataframe struct {
	Rows   []row.Row
	Schema schema.Schema

	updateValues bool
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

	if df.Schema.Equals(schema.Schema{}) && len(df.Rows) == 0 {
		// This is the case: df = Dataframe{}
		return true
	}

	if df.Schema.Equals(schema.Schema{}) && len(df.Rows) == 1 && df.Rows[0].Equals(row.Row{}) {
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

func (df *Dataframe) Shape() [2]int {
	return [2]int{
		len(df.Rows),
		len(df.Schema.Columns),
	}
}

func (df *Dataframe) RSlice(sIdx int, eIdx int) Dataframe {

	nDfRows := len(df.Rows)
	if eIdx > nDfRows {
		eIdx = nDfRows
	}

	if sIdx > eIdx {
		panic(fmt.Sprintf("Invadid dataframe slice indexes, start: %d, end: %d", sIdx, eIdx))
	}

	if sIdx == eIdx {
		return Dataframe{}
	}

	newDf := Dataframe{}
	newDf.Rows = df.Rows[sIdx:eIdx]
	newDf.Schema = df.Schema

	return newDf
}

func (df *Dataframe) CSlice(sIdx int, eIdx int) Dataframe {

	nDfColumns := len(df.Schema.Columns)
	if eIdx > nDfColumns {
		eIdx = nDfColumns
	}

	if sIdx > eIdx {
		panic(fmt.Sprintf("Invadid dataframe slice indexes, start: %d, end: %d", sIdx, eIdx))
	}

	if sIdx == eIdx {
		return Dataframe{}
	}

	newDf := Dataframe{}
	newDf.Schema.Columns = df.Schema.Columns[sIdx:eIdx]
	for _, r := range df.Rows {
		newDf.Rows = append(newDf.Rows, r.Slice(sIdx, eIdx))
	}

	return newDf
}

func (df *Dataframe) updateDataframeSchema(r row.Row) {

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

func (df *Dataframe) updateColumnsTypesBasedOnSchema(s schema.Schema) {
	for idx, c := range s.Columns {
		if !(types.IsType(df.Schema.Columns[idx].Type, c.Type)) {
			df.updateColumnTypeInPosition(idx, c.Type)
			df.updateValues = true
		}
	}
}

func (df *Dataframe) updateDfSchemaFromRowWithmLargerSchema(r row.Row) {

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
		df.Rows[idx].Schema = df.Schema
	}

}

func (df *Dataframe) applyDfSchemaInRowWithSmallerSchema(r *row.Row) {

	// Add dummy values to the row in order to alighn with the df larger schema
	rowLen := len(r.Schema.Columns)
	var dummyValues []interface{}

	for _, c := range df.Schema.Columns[rowLen:] {

		if types.IsType(c.Type, types.Int) {
			dummyValues = append(dummyValues, math.NaN())
			r.Schema.Columns = append(r.Schema.Columns, column.Column{Name: "", Type: types.Int})
		} else if types.IsType(c.Type, types.Float) {
			dummyValues = append(dummyValues, math.NaN())
			r.Schema.Columns = append(r.Schema.Columns, column.Column{Name: "", Type: types.Float})
		} else if types.IsType(c.Type, types.String) {
			dummyValues = append(dummyValues, "")
			r.Schema.Columns = append(r.Schema.Columns, column.Column{Name: "", Type: types.String})
		} else if types.IsType(c.Type, types.Bool) {
			dummyValues = append(dummyValues, "")
			r.Schema.Columns = append(r.Schema.Columns, column.Column{Name: "", Type: types.String}) // because "" was used as dummy value for the missing bool value
		}
	}

	// Append dummy values to the row
	df.Rows[len(df.Rows)-1].Values = append(df.Rows[len(df.Rows)-1].Values, dummyValues...)
	df.Rows[len(df.Rows)-1].Schema = df.Schema

}

func (df *Dataframe) updateColumnTypeInPosition(idx int, t types.DataType) {

	switch df.Schema.Columns[idx].Type {
	case types.Int:
		switch t {
		case types.Float:
			df.Schema.Columns[idx].Type = types.Float
		default:
			df.Schema.Columns[idx].Type = types.String
		}
	case types.Float:
		switch t {
		case types.Int:
			df.Schema.Columns[idx].Type = types.Float
		default:
			df.Schema.Columns[idx].Type = types.String
		}
	default:
		df.Schema.Columns[idx].Type = types.String
	}

}

func (df *Dataframe) updateValuesFormat() {
	for _, r := range df.Rows {
		for cIdx, c := range df.Schema.Columns {
			if !types.IsType(r.Values[cIdx], c.Type) {
				switch c.Type {
				case types.Int:
					if v, ok := r.Values[cIdx].(float64); ok {
						r.Values[cIdx] = int(v)
					} else if v, ok := r.Values[cIdx].(float32); ok {
						r.Values[cIdx] = int(v)
					}
				case types.Float:
					if v, ok := r.Values[cIdx].(int); ok {
						r.Values[cIdx] = float64(v)
					} else if v, ok := r.Values[cIdx].(int8); ok {
						r.Values[cIdx] = float64(v)
					} else if v, ok := r.Values[cIdx].(int16); ok {
						r.Values[cIdx] = float64(v)
					} else if v, ok := r.Values[cIdx].(int32); ok {
						r.Values[cIdx] = float64(v)
					} else if v, ok := r.Values[cIdx].(int64); ok {
						r.Values[cIdx] = float64(v)
					}
				default:
					r.Values[cIdx] = fmt.Sprintf("%v", r.Values[cIdx])
				}
			}
		}
	}
}

func SchemaOK(i interface{}, s schema.Schema) bool {

	// Evaluate schema for Row struct
	if r, ok := i.(row.Row); ok {
		return r.SchemaOK(s)
	}

	// Evaluate schema for raw data
	if d, ok := i.([]interface{}); ok {
		return schema.SchemaOKForRawData(d, s)
	}

	return false
}

func createDataframeWithNoSchemaInfo(rows *[][]interface{}) Dataframe {

	df := Dataframe{}

	for _, r := range *rows {

		row := row.CreateRow(&r)
		schema := row.Schema
		df.Rows = append(df.Rows, row)

		// Initialise schema
		if len(df.Schema.Columns) == 0 {
			df.Schema = schema
		}

		// Update schema - if needed
		if !SchemaOK(&row, df.Schema) {
			df.updateDataframeSchema(row)
		}

	}

	if df.updateValues {
		df.updateValuesFormat()
	}

	return df

}

func createDataframeWithColumnNames(rows *[][]interface{}, c []string) Dataframe {

	df := Dataframe{}

	for _, r := range *rows {
		row := row.CreateRow(&r, c)
		schema := row.Schema
		df.Rows = append(df.Rows, row)
		if len(df.Schema.Columns) == 0 {
			df.Schema = schema
		} // Initialise schema

		if !SchemaOK(&row, df.Schema) {
			df.updateDataframeSchema(row)
		} // Update schema - if needed
	}

	if df.updateValues {
		df.updateValuesFormat()
	}

	return df

}

func createDataframeWithSchema(rows *[][]interface{}, s schema.Schema) Dataframe {

	df := Dataframe{}
	df.Schema = s

	for _, r := range *rows {
		row := row.CreateRow(&r, s)
		df.Rows = append(df.Rows, row)
	}

	return df
}

func CreateDataframe(rows *[][]interface{}, i ...interface{}) Dataframe {

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
	return createDataframeWithSchema(rows, input.(schema.Schema))
}
