package dataframe

import (
	"parallel/column"
	"parallel/schema"
	"parallel/types"
	"testing"
)

func TestDistinct(t *testing.T) {

	rawValues := [][]interface{}{
		{1, 2, false, "a", 5, true},
		{1, 2, false, "b"},
		{1, 2, false, "c"},
		{2, 2, false, "d"},
		{2, 2, true},
		{2, 2, false, "e"},
		{3, 2, false, "f"},
		{3, 2, false, 5, 5, 5.5, true},
		{4, 2, true},
	}

	df := CreateDataframe(rawValues)
	distictVal := df.Distinct("column_0", "column_1")
	distinctDf := distictVal.AsDataframe()
	distinctDf = distinctDf.Sort("column_0")

	expectedDistinctDf := CreateDataframe(
		[][]interface{}{
			{1, 2},
			{2, 2},
			{3, 2},
			{4, 2},
		},
		schema.Schema{
			Columns: []column.Column{
				{Name: "column_0", Type: types.Int},
				{Name: "column_1", Type: types.Int},
			},
		},
	)
	if !(distinctDf.Equals(expectedDistinctDf)) {
		t.Error("Unexpected distinct values dataframe.")
	}

}

func TestDistinctSingleRecord(t *testing.T) {

	rawValues := [][]interface{}{
		{1, 2, false, "a", 5, true},
	}

	df := CreateDataframe(rawValues)
	distictVal := df.Distinct("column_0", "column_1")
	distinctDf := distictVal.AsDataframe()

	expectedDistinctDf := CreateDataframe(
		[][]interface{}{{1, 2}},
		schema.Schema{
			Columns: []column.Column{
				{Name: "column_0", Type: types.Int},
				{Name: "column_1", Type: types.Int},
			},
		},
	)
	if !(distinctDf.Equals(expectedDistinctDf)) {
		t.Error("Unexpected distinct values dataframe.")
	}

}
