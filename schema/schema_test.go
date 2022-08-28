package schema

import (
	"parallel/column"
	// "parallel/row"
	"parallel/types"
	"testing"
)

func TestSchemaEquality(t *testing.T) {

	s1 := Schema{
		Columns: []column.Column{
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
		Columns: []column.Column{
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
		Columns: []column.Column{
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

	if s1.Equals(s2) {
		t.Error("Schemas should not be equal.")
	}
}

func TestSchemaNotEqualName(t *testing.T) {

	s1 := Schema{
		Columns: []column.Column{
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
		Columns: []column.Column{
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

	if !SchemaOKForRawData(values, schema) {
		t.Error("Value should comply with the schema.")
	}

}
