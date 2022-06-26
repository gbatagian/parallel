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
	for idx, r := range df1.Rows {
		if !r.Equals(df2.Rows[idx]) {
			return false
		}
	}
	return true
}

func (df *Dataframe) updateDataframeSchema(r Row) {

	df_schema_len := len(df.Schema.Columns)
	row_schema_len := len(r.Schema.Columns)

	if df_schema_len < row_schema_len {
		// row has a larger schema (more values than the df columnd)
		df.updateDfSchemaFromRowWithmLargerSchema(r)
	} else {
		// row has a smaller schema (less values than the df columnd)
		df.applyDfSchemaInRowWithSmallerSchema(r)

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
	df_schema_len := len(df.Schema.Columns)
	df.Schema.Columns = append(df.Schema.Columns, r.Schema.Columns[df_schema_len:]...)
	var dummy_values []interface{}

	// Create dummy values to populate the extra columns on the previous rows
	for _, c := range r.Schema.Columns[df_schema_len:] {
		if types.IsType(c.Type, types.Int) {
			dummy_values = append(dummy_values, math.NaN())
		} else if types.IsType(c.Type, types.Float) {
			dummy_values = append(dummy_values, math.NaN())
		} else {
			dummy_values = append(dummy_values, "")
		}
	}
	// Populate previous rows with dummy values
	for idx, r := range df.Rows[:len(df.Rows)-1] {
		df.Rows[idx].Values = append(r.Values, dummy_values...)
	}

}

func (df *Dataframe) applyDfSchemaInRowWithSmallerSchema(r Row) {

	// Add dummy values to the row in order to complu with the df larger schema
	row_len := len(r.Schema.Columns)
	var dummy_values []interface{}

	for _, c := range df.Schema.Columns[row_len:] {
		if types.IsType(c.Type, types.Int) {
			dummy_values = append(dummy_values, math.NaN())
		} else if types.IsType(c.Type, types.Float) {
			dummy_values = append(dummy_values, math.NaN())
		} else {
			dummy_values = append(dummy_values, "")
		}
	}

	df.Rows[len(df.Rows)-1].Values = append(df.Rows[len(df.Rows)-1].Values, dummy_values...)

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

		if SchemaOK(row, df.Schema) || len(df.Schema.Columns) == 0 {
			df.Rows = append(df.Rows, row)
			if len(df.Schema.Columns) == 0 {
				df.Schema = schema
			}
		} else {
			df.Rows = append(df.Rows, row)
			df.updateDataframeSchema(row)
		}

	}

	return df

}

func createDataframeWithSchemaInfo(rows [][]interface{}, i interface{}) Dataframe {

	df := Dataframe{}

	// Case 1: The provided info is the column names.
	if _, ok := i.([]string); ok {
		for _, r := range rows {
			df.Rows = append(df.Rows, CreateRow(r))
		}
	}

	return df

}

func CreateDatafeme(rows [][]interface{}, i ...interface{}) Dataframe {

	// Case1: No schema related information was provided in dataframe definition.
	if len(i) == 0 {
		return createDataframeWithNoSchemaInfo(rows)
	}

	return createDataframeWithSchemaInfo(rows, i)
}
