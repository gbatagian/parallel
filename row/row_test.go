package row

import (
	"parallel/column"
	"parallel/schema"
	"parallel/types"
	"testing"
)

func TestRowCreationWithSchema(t *testing.T) {

	schema := schema.Schema{
		Columns: []column.Column{
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
	rawData := []interface{}{"A", 1}

	row := CreateRow(rawData, schema)

	if !row.Equals(Row{Values: rawData, Schema: schema}) {
		t.Error("Rows should be equal.")
	}
}

func TestRowCreationWithNonSchema(t *testing.T) {

	values := []interface{}{"A", 1, 1.444, false}
	row := CreateRow(values)

	if !row.Equals(
		Row{
			Values: values,
			Schema: schema.Schema{
				Columns: []column.Column{
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
	) {
		t.Error("Rows should be equal.")
	}

}

func TestRowCreationWithColumnNames(t *testing.T) {

	values := []interface{}{"A", 1, 1.444, false}
	row := CreateRow(
		values,
		[]string{"A", "B", "C", "D"},
	)

	if !row.Equals(
		Row{
			Values: values,
			Schema: schema.Schema{
				Columns: []column.Column{
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
	) {
		t.Error("Rows should be equal.")
	}

}

func TestSchemaForRow(t *testing.T) {

	values := []interface{}{1, 1.1, "a", true}
	schema := schema.Schema{
		Columns: []column.Column{
			{
				Name: "a",
				Type: types.Int,
			},
			{
				Name: "b",
				Type: types.Float,
			},
			{
				Name: "c",
				Type: types.String,
			},
			{
				Name: "d",
				Type: types.Bool,
			},
		},
	}

	row := Row{
		Values: values,
		Schema: schema,
	}

	if !row.SchemaOK(schema) {
		t.Error("Row should comply with the schema.")
	}

}
