package dataframe

import (
	"math"
	"parallel/types"
	"testing"
)

func TestDataframeCreationWithNoSchema(t *testing.T) {

	raw_values := [][]interface{}{
		{1},
		{"2.1", "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a", true},
		{"2.1", "2022-06-01 19:58:30.991242+00", "b", true},
		{5},
		{true, 2},
		{"2.1", "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a", "true"},
		{"2.1", "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a", true},
	}

	df := CreateDatafeme(raw_values)

	expected_df_schema := Schema{
		Columns: []Column{
			{
				Name: "column_0",
				Type: types.String,
			},
			{
				Name: "column_1",
				Type: types.String,
			},
			{
				Name: "column_2",
				Type: types.String,
			},
			{
				Name: "column_3",
				Type: types.Bool,
			},
			{
				Name: "column_4",
				Type: types.Float,
			},
			{
				Name: "column_5",
				Type: types.Int,
			},
			{
				Name: "column_6",
				Type: types.String,
			},
			{
				Name: "column_7",
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

	df := CreateDatafeme(raw_values, []string{"a", "b", "c", "d", "e", "f", "g", "h"})

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
	df := CreateDatafeme(raw_values, schema)

	if !df.Schema.Equals(schema) {
		t.Error("Schemas should be equal.")
	}
}
