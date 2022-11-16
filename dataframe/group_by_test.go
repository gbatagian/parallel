package dataframe

import (
	"parallel/core"
	"parallel/row"
	"testing"
)

func TestGroupBy(t *testing.T) {

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

	df := CreateDataframe(&rawValues)
	gb := df.GroupBy("column_0", "column_1")

	expectedGroupKeys := make(map[string]core.Key)
	expectedGroupDfs := make(map[string]Dataframe)

	gk := core.Key{Values: []interface{}{1, 2}}
	expectedGroupKeys[gk.Hash()] = gk
	expectedGroupDfs[gk.Hash()] = Dataframe{Rows: df.Rows[0:3], Schema: df.Schema}

	gk = core.Key{Values: []interface{}{2, 2}}
	expectedGroupKeys[gk.Hash()] = gk
	expectedGroupDfs[gk.Hash()] = Dataframe{Rows: df.Rows[3:6], Schema: df.Schema}

	gk = core.Key{Values: []interface{}{3, 2}}
	expectedGroupKeys[gk.Hash()] = gk
	expectedGroupDfs[gk.Hash()] = Dataframe{Rows: df.Rows[6:8], Schema: df.Schema}

	gk = core.Key{Values: []interface{}{4, 2}}
	expectedGroupKeys[gk.Hash()] = gk
	expectedGroupDfs[gk.Hash()] = Dataframe{Rows: []row.Row{df.Rows[8]}, Schema: df.Schema}

	for k, gdf := range gb.Groups {

		expectedGroupKey := expectedGroupKeys[k.Hash()]
		if !(k.Hash() == expectedGroupKey.Hash()) {
			t.Error("Unexpected group.")
		}

		expectedGroupDf := expectedGroupDfs[k.Hash()]
		expectedGroupDf = expectedGroupDf.Sort("column_3")
		gdf = gdf.Sort("column_3")
		if !gdf.Equals(expectedGroupDf) {
			t.Error("Unexcpected group dataframe.")
		}
	}
}

func TestGroupBySingleRecord(t *testing.T) {

	rawValues := [][]interface{}{
		{1, 2, false, "a", 5, true},
	}

	df := CreateDataframe(&rawValues)
	gb := df.GroupBy("column_0", "column_1")

	expected_gk := core.Key{Values: []interface{}{1, 2}}
	expected_gDf := Dataframe{Rows: []row.Row{df.Rows[0]}, Schema: df.Schema}

	for k, gdf := range gb.Groups {

		if !(k.Hash() == expected_gk.Hash()) {
			t.Error("Unexpected group.")
		}

		if !gdf.Equals(expected_gDf) {
			t.Error("Unexcpected group dataframe.")
		}
	}
}
