package dataframe

import (
	"math"
	"parallel/column"
	"parallel/schema"
	"parallel/types"
	"testing"
)

func TestDataframeCreationWithNoSchema(t *testing.T) {

	var rawValues [][]interface{}
	var df Dataframe
	var expectedDfSchema schema.Schema

	// Case 1: mix
	rawValues = [][]interface{}{
		{1, 2.1},
		{2, 2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, 1, true},
		{3, 2.1, "2022-06-01 19:58:30.991242+00", "b", true},
		{4},
		{5, 2.1, 2},
		{6, 2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a", "true"},
		{7, 2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, 1, true},
	}

	df = CreateDataframe(&rawValues)

	expectedDfSchema = schema.Schema{
		Columns: []column.Column{
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

	if !df.Schema.Equals(expectedDfSchema) {
		t.Error("Schemas should be equal.")
	}

	// Case 2: row with missing bool values - BUGFIX
	rawValues = [][]interface{}{
		{1, false},
		{2},
	}

	df = CreateDataframe(&rawValues)

	expectedDfSchema = schema.Schema{
		Columns: []column.Column{
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

	if !df.Schema.Equals(expectedDfSchema) {
		t.Error("Schemas should be equal.")
	}

	// Case 3: row with extra bool value - BUGFIX
	rawValues = [][]interface{}{
		{2},
		{1, false},
	}

	df = CreateDataframe(&rawValues)

	expectedDfSchema = schema.Schema{
		Columns: []column.Column{
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

	if !df.Schema.Equals(expectedDfSchema) {
		t.Error("Schemas should be equal.")
	}

}

func TestDataframeCreationWithColumnNames(t *testing.T) {

	rawValues := [][]interface{}{
		{2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a", true},
		{"2.1", "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a", true},
		{2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a", true},
		{"2.1", "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a", "true"},
	}

	df := CreateDataframe(&rawValues, []string{"a", "b", "c", "d", "e", "f", "g", "h"})

	expectedDfSchema := schema.Schema{
		Columns: []column.Column{
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

	if !df.Schema.Equals(expectedDfSchema) {
		t.Error("Schemas should be equal.")
	}

}

func TestDataframeCreationWithSchema(t *testing.T) {

	rawValues := [][]interface{}{
		{2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a"},
		{2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a"},
		{2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a"},
		{math.NaN(), "2022-06-01 19:58:30.991242+00", "b", true, 2.2, math.NaN(), "a"},
	}

	schema := schema.Schema{
		Columns: []column.Column{
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
	df := CreateDataframe(&rawValues, schema)

	if !df.Schema.Equals(schema) {
		t.Error("Schemas should be equal.")
	}
}

func TestDataframeColumnNames(t *testing.T) {

	schema := schema.Schema{
		Columns: []column.Column{
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
		&[][]interface{}{
			{1, 0},
			{0, 1},
		},
		schema,
	)

	names := df.ColumnNames()
	expectedNames := []string{"a", "b"}

	for idx := range names {
		if !(names[idx] == expectedNames[idx]) {
			t.Error("Column names should be equal.")
		}
	}

}

func TestDataframeColumnTypes(t *testing.T) {

	schema := schema.Schema{
		Columns: []column.Column{
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
		&[][]interface{}{
			{1, 0},
			{0, 1},
		},
		schema,
	)

	tps := df.ColumnTypes()
	expectedTypes := []types.DataType{types.Int, types.Int}

	for idx := range tps {
		if !(tps[idx] == expectedTypes[idx]) {
			t.Error("Column types should be equal.")
		}
	}

}
