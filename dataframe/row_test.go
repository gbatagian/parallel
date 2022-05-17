package dataframe

import (
	"parallel/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRowCreationWithSchema(t *testing.T) {

	schema := Schema{
		Columns: []Column{
			{
				Name: "var1",
				Type: types.String,
			},
			{
				Name: "var2",
				Type: types.Int,
			},
		},
	}
	raw_data := []interface{}{"A", 1}

	row := CreateRow(raw_data, schema)

	assert.Equal(t, row, Row{Values: raw_data, Schema: schema})
}

func TestRowCreationWithNonSchema(t *testing.T) {

	values := []interface{}{"A", 1, 1.444, false}
	row := CreateRow(values)

	assert.Equal(
		t, row,
		Row{
			Values: values,
			Schema: Schema{
				Columns: []Column{
					{
						Name: "column_0",
						Type: types.String,
					},
					{
						Name: "column_1",
						Type: types.Int,
					},
					{
						Name: "column_2",
						Type: types.Float,
					},
					{
						Name: "column_3",
						Type: types.Bool,
					},
				},
			},
		},
	)

}

func TestRowCreationWithColumnNames(t *testing.T) {

	values := []interface{}{"A", 1, 1.444, false}
	row := CreateRow(
		values,
		[]string{"A", "B", "C", "D"},
	)

	assert.Equal(
		t, row,
		Row{
			Values: values,
			Schema: Schema{
				Columns: []Column{
					{
						Name: "A",
						Type: types.String,
					},
					{
						Name: "B",
						Type: types.Int,
					},
					{
						Name: "C",
						Type: types.Float,
					},
					{
						Name: "D",
						Type: types.Bool,
					},
				},
			},
		},
	)

}
