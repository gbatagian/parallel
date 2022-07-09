package dataframe

import (
	"parallel/types"
	"testing"
)

func TestSchemaEquality(t *testing.T) {

	s1 := Schema{
		Columns: []Column{
			{
				Name: "a",
				Type: types.Int,
			},
			{
				Name: "b",
				Type: types.Float,
			},
		},
	}

	s2 := Schema{
		Columns: []Column{
			{
				Name: "a",
				Type: types.Int,
			},
			{
				Name: "b",
				Type: types.Float,
			},
		},
	}

	if !s1.Equals(s2) {
		t.Error("Schemas should be equal.")
	}
}

func TestSchemaNotEqualType(t *testing.T) {

	s1 := Schema{
		Columns: []Column{
			{
				Name: "a",
				Type: types.Int,
			},
			{
				Name: "b",
				Type: types.Float,
			},
		},
	}

	s2 := Schema{
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

	if s1.Equals(s2) {
		t.Error("Schemas should not be equal.")
	}
}

func TestSchemaNotEqualName(t *testing.T) {

	s1 := Schema{
		Columns: []Column{
			{
				Name: "a",
				Type: types.Int,
			},
			{
				Name: "b",
				Type: types.Float,
			},
		},
	}

	s2 := Schema{
		Columns: []Column{
			{
				Name: "a",
				Type: types.Int,
			},
			{
				Name: "B",
				Type: types.Float,
			},
		},
	}

	if s1.Equals(s2) {
		t.Error("Schemas should not be equal.")
	}
}

func TestSchemaForRawData(t *testing.T) {

	values := []interface{}{1, 1.1, "a", true}

	schema := Schema{
		Columns: []Column{
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

	if !SchemaOK(values, schema) {
		t.Error("Value should comply with the schema.")
	}

}

func TestSchemaForRow(t *testing.T) {

	values := []interface{}{1, 1.1, "a", true}
	schema := Schema{
		Columns: []Column{
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

	if !SchemaOK(row, schema) {
		t.Error("Row should comply with the schema.")
	}

}
