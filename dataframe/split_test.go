package dataframe

import "testing"

func TestSplit(t *testing.T) {

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
	dfPackets := df.Split(3)

	expectedDfPackets := []Dataframe{
		{Rows: df.Rows[:3], Schema: df.Schema},
		{Rows: df.Rows[3:6], Schema: df.Schema},
		{Rows: df.Rows[6:], Schema: df.Schema},
	}

	for idx, dfPacket := range dfPackets {
		if !dfPacket.Equals(expectedDfPackets[idx]) {
			t.Error("Unexpected dtaframe paacket")
		}
	}
}
func TestSplitPacketsMoreThanRows(t *testing.T) {

	rawValues := [][]interface{}{}

	df := CreateDataframe(rawValues)
	dfPackets := df.Split(3)

	if len(dfPackets) != 0 {
		t.Error("Unexpected dtaframe paackets")
	}

}

func TestSplitEmptyDf(t *testing.T) {

	rawValues := [][]interface{}{
		{1, 2, false, "a", 5, true},
		{1, 2, false, "b"},
		{1, 2, false, "c"},
	}

	df := CreateDataframe(rawValues)
	dfPackets := df.Split(30)

	expectedDfPackets := []Dataframe{
		{Rows: df.Rows[:1], Schema: df.Schema},
		{Rows: df.Rows[1:2], Schema: df.Schema},
		{Rows: df.Rows[2:], Schema: df.Schema},
	}

	for idx, dfPacket := range dfPackets {
		if !dfPacket.Equals(expectedDfPackets[idx]) {
			t.Error("Unexpected dtaframe paacket")
		}
	}
}
