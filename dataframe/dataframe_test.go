package dataframe

import (
	"math"
	"parallel/types"
	"testing"
)

func TestDataframeCreationWithNoSchema(t *testing.T) {

	var raw_values [][]interface{}
	var df Dataframe
	var expected_df_schema Schema

	// Case 1: mix
	raw_values = [][]interface{}{
		{1, 2.1},
		{2, 2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, 1, true},
		{3, 2.1, "2022-06-01 19:58:30.991242+00", "b", true},
		{4},
		{5, 2.1, 2},
		{6, 2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a", "true"},
		{7, 2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, 1, true},
	}

	df = CreateDataframe(raw_values)

	expected_df_schema = Schema{
		Columns: []Column{
			{
				Name: "column_0",
				Type: types.Int,
			},
			{
				Name: "column_1",
				Type: types.Float,
			},
			{
				Name: "column_2",
				Type: types.String,
			},
			{
				Name: "column_3",
				Type: types.String,
			},
			{
				Name: "column_4",
				Type: types.String,
			},
			{
				Name: "column_5",
				Type: types.Float,
			},
			{
				Name: "column_6",
				Type: types.Int,
			},
			{
				Name: "column_7",
				Type: types.String,
			},
			{
				Name: "column_8",
				Type: types.String,
			},
		},
	}

	if !df.Schema.Equals(expected_df_schema) {
		t.Error("Schemas should be equal.")
	}

	// Case 2: row with missing bool values - BUGFIX
	raw_values = [][]interface{}{
		{1, false},
		{2},
	}

	df = CreateDataframe(raw_values)

	expected_df_schema = Schema{
		Columns: []Column{
			{
				Name: "column_0",
				Type: types.Int,
			},
			{
				Name: "column_1",
				Type: types.String,
			},
		},
	}

	if !df.Schema.Equals(expected_df_schema) {
		t.Error("Schemas should be equal.")
	}

	// Case 3: row with extra bool value - BUGFIX
	raw_values = [][]interface{}{
		{2},
		{1, false},
	}

	df = CreateDataframe(raw_values)

	expected_df_schema = Schema{
		Columns: []Column{
			{
				Name: "column_0",
				Type: types.Int,
			},
			{
				Name: "column_1",
				Type: types.String,
			},
		},
	}

	if !df.Schema.Equals(expected_df_schema) {
		t.Error("Schemas should be equal.")
	}

}

func TestDataframeCreationWithColumnNames(t *testing.T) {

	raw_values := [][]interface{}{
		{2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a", true},
		{"2.1", "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a", true},
		{2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a", true},
		{"2.1", "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a", "true"},
	}

	df := CreateDataframe(raw_values, []string{"a", "b", "c", "d", "e", "f", "g", "h"})

	expected_df_schema := Schema{
		Columns: []Column{
			{
				Name: "a",
				Type: types.String,
			},
			{
				Name: "b",
				Type: types.String,
			},
			{
				Name: "c",
				Type: types.String,
			},
			{
				Name: "d",
				Type: types.Bool,
			},
			{
				Name: "e",
				Type: types.Float,
			},
			{
				Name: "f",
				Type: types.Int,
			},
			{
				Name: "g",
				Type: types.String,
			},
			{
				Name: "h",
				Type: types.String,
			},
		},
	}

	if !df.Schema.Equals(expected_df_schema) {
		t.Error("Schemas should be equal.")
	}

}

func TestDataframeCreationWithSchema(t *testing.T) {

	raw_values := [][]interface{}{
		{2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a"},
		{2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a"},
		{2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a"},
		{math.NaN(), "2022-06-01 19:58:30.991242+00", "b", true, 2.2, math.NaN(), "a"},
	}

	schema := Schema{
		Columns: []Column{
			{
				Type: types.Float,
				Name: "a",
			},
			{
				Type: types.String,
				Name: "b",
			},
			{
				Type: types.String,
				Name: "c",
			},
			{
				Type: types.Bool,
				Name: "d",
			},
			{
				Type: types.Float,
				Name: "e",
			},
			{
				Type: types.Int,
				Name: "f",
			},
			{
				Type: types.String,
				Name: "g",
			},
		},
	}
	df := CreateDataframe(raw_values, schema)

	if !df.Schema.Equals(schema) {
		t.Error("Schemas should be equal.")
	}
}

func TestDataframeColumnNames(t *testing.T) {

	schema := Schema{
		Columns: []Column{
			{
				Name: "a",
				Type: types.Int,
			},
			{
				Name: "b",
				Type: types.Int,
			},
		},
	}

	df := CreateDataframe(
		[][]interface{}{
			{1, 0},
			{0, 1},
		},
		schema,
	)

	names := df.ColumnNames()
	expected_names := []string{"a", "b"}

	for idx := range names {
		if !(names[idx] == expected_names[idx]) {
			t.Error("Column names should be equal.")
		}
	}

}

func TestDataframeColumnTypes(t *testing.T) {

	schema := Schema{
		Columns: []Column{
			{
				Name: "a",
				Type: types.Int,
			},
			{
				Name: "b",
				Type: types.Int,
			},
		},
	}

	df := CreateDataframe(
		[][]interface{}{
			{1, 0},
			{0, 1},
		},
		schema,
	)

	tps := df.ColumnTypes()
	expected_types := []types.DataType{types.Int, types.Int}

	for idx := range tps {
		if !(tps[idx] == expected_types[idx]) {
			t.Error("Column types should be equal.")
		}
	}

}
