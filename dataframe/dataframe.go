package dataframe

import (
	"fmt"
	"math"
	"os"
	"parallel/types"
	"strings"
	"text/tabwriter"
)

type Dataframe struct {
	Rows   []Row
	Schema Schema
}

func (df *Dataframe) updateDataframeSchema(s Schema) {

	if len(df.Schema.Columns) != len(s.Columns) {
		df.updateUnevenLengthSchema(s)
	}

	for idx, c := range s.Columns {
		if !(types.IsType(df.Schema.Columns[idx].Type, c.Type)) {
			df.updateColumnTypeInPosition(idx, c.Type)
		}
	}

}

func (df *Dataframe) updateUnevenLengthSchema(s Schema) {

	df_row_len := len(df.Schema.Columns)
	df.Schema.Columns = append(df.Schema.Columns, s.Columns[df_row_len:]...)
	var dummy_values []interface{}

	for _, c := range s.Columns[df_row_len:] {
		if types.IsType(c.Type, types.Int) {
			dummy_values = append(dummy_values, math.NaN())
		} else if types.IsType(c.Type, types.Float) {
			dummy_values = append(dummy_values, math.NaN())
		} else {
			dummy_values = append(dummy_values, "")
		}
	}
	for idx, r := range df.Rows[:len(df.Rows)-1] {
		df.Rows[idx].Values = append(r.Values, dummy_values...)
	}

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
				v1, ok1 := r.Values[idx].(float64)
				v2, ok2 := r.Values[idx].(float32)
				if ok1 {
					r.Values[idx] = int(v1)
				} else if ok2 {
					r.Values[idx] = int(v2)
				}
			}
		}
	case types.Float:
		for _, r := range df.Rows {
			if !types.IsType(r.Values[idx], f) {
				v1, ok1 := r.Values[idx].(int)
				v2, ok2 := r.Values[idx].(int8)
				v3, ok3 := r.Values[idx].(int16)
				v4, ok4 := r.Values[idx].(int32)
				v5, ok5 := r.Values[idx].(int64)
				if ok1 {
					r.Values[idx] = float64(v1)
				} else if ok2 {
					r.Values[idx] = float64(v2)
				} else if ok3 {
					r.Values[idx] = float64(v3)
				} else if ok4 {
					r.Values[idx] = float64(v4)
				} else if ok5 {
					r.Values[idx] = float64(v5)
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
			df.updateDataframeSchema(row.Schema)
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

	// Case1: No schema related information was provided in dataframe definition
	if len(i) == 0 {
		return createDataframeWithNoSchemaInfo(rows)
	}

	return createDataframeWithSchemaInfo(rows, i)
}

func (df *Dataframe) Print() {
	w := tabwriter.NewWriter(os.Stdout, 5, 100, 1, ' ', tabwriter.Debug)

	var column_names []interface{}
	var column_types []interface{}
	for _, c := range df.Schema.Columns {
		column_names = append(column_names, c.Name)
		column_types = append(column_types, c.Type)
	}
	fmt.Fprintf(w, strings.Repeat("%v\t", len(df.Schema.Columns))+"\n", column_names...)
	fmt.Fprintf(w, strings.Repeat("(type: %v)\t", len(df.Schema.Columns))+"\n", column_types...)

	for _, r := range df.Rows {
		fmt.Fprintf(w, strings.Repeat("%v\t", len(df.Schema.Columns))+"\n", r.Values...)
	}
	w.Flush()
}
