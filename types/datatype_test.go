package types

import (
	"fmt"
	"math"
	"testing"
)

func TestDataTypeString(t *testing.T) {

	s := "sample string"

	if !IsType(s, String) {
		t.Error("Failed to evaluate string input correclty.")
	}

}

func TestDataTypeInt(t *testing.T) {

	i := 119

	if !IsType(i, Int) {
		t.Error("Failed to evaluate integer input correclty.")
	}

}

func TestDataTypeFloat(t *testing.T) {

	f := 119.1992

	if !IsType(f, Float) {
		t.Error("Failed to evaluate float input correclty.")
	}

}

func TestDataTypeBool(t *testing.T) {

	b := true

	if !IsType(b, Bool) {
		t.Error("Failed to evaluate boolean input correclty.")
	}

}

func TestDataTypeNaNIsInt(t *testing.T) {

	nan := math.NaN()

	if !IsType(nan, Int) {
		t.Error("Failed to evaluate nan as integer.")
	}

}

func TestDataTypeNaNIsFloat(t *testing.T) {

	nan := math.NaN()

	if !IsType(nan, Float) {
		t.Error("Failed to evaluate nan as float.")
	}

}

func TestDataTypeEvaluationForDataType(t *testing.T) {

	dataTypes := []DataType{Int, Float, String, Bool}

	for _, tp := range dataTypes {

		if !IsType(tp, tp) {
			msg := fmt.Sprintf("Failed to evaluate %v DataType", tp)
			t.Error(msg)
		}
	}

}
