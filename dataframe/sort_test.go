package dataframe

import (
	"parallel/column"
	"parallel/schema"
	"parallel/types"
	"testing"
)

func TestSort(t *testing.T) {

	raw_values := [][]interface{}{
		{1, 2, false, "a"},
		{1, 2, false, "b"},
		{1, 2, false, "c"},
		{2, 2, false, "d"},
		{2, 2, true},
		{2, 2, false, "e"},
		{1, 2, false, "f"},
		{1, 2, false, "g"},
		{1, 2, true},
	}

	df := CreateDataframe(&raw_values)

	sd := df.Sort("column_0", "~column_2", "column_3")

	expected_df := CreateDataframe(
		&[][]interface{}{
			{1, 2, false, "a"},
			{1, 2, false, "b"},
			{1, 2, false, "c"},
			{1, 2, false, "f"},
			{1, 2, true, ""},
			{2, 2, false, "d"},
			{2, 2, false, "e"},
			{2, 2, true, ""},
		},
		schema.Schema{
			Columns: []column.Column{
				{Name: "column_0", Type: types.Int},
				{Name: "column_1", Type: types.Int},
				{Name: "column_2", Type: types.Bool},
				{Name: "column_4", Type: types.String},
			},
		},
	)

	if sd.Equals(expected_df) {
		t.Error("Unexpected dataframe after sort operation.")
	}

}
