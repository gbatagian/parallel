package dataframe

import (
	"parallel/types"
	"testing"
)

func TestColumnEquality(t *testing.T) {

	c1 := Column{Name: "a", Type: types.Int}
	c2 := Column{Name: "a", Type: types.Int}

	if !c1.Equals(c2) {
		t.Error("Columns should be equal.")
	}

}

func TestColumnNotEqualType(t *testing.T) {

	c1 := Column{Name: "a", Type: types.Int}
	c2 := Column{Name: "a", Type: types.Float}

	if c1.Equals(c2) {
		t.Error("Columns should not be equal.")
	}

}

func TestColumnNotEqualName(t *testing.T) {

	c1 := Column{Name: "a", Type: types.Int}
	c2 := Column{Name: "A", Type: types.Int}

	if c1.Equals(c2) {
		t.Error("Columns should not be equal.")
	}

}
