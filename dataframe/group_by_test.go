package dataframe

import (
	"fmt"
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

	df := CreateDataframe(rawValues)
	gb := df.GroupBy("column_0", "column_1")

	expectedGroupKeys := make(map[string]GroupKey)
	expectedGroupDfs := make(map[string]Dataframe)

	gk := GroupKey{[]interface{}{1, 2}}
	expectedGroupKeys[gk.Hash()] = gk
	expectedGroupDfs[gk.Hash()] = Dataframe{df.Rows[0:3], df.Schema}

	gk = GroupKey{[]interface{}{2, 2}}
	expectedGroupKeys[gk.Hash()] = gk
	expectedGroupDfs[gk.Hash()] = Dataframe{df.Rows[3:6], df.Schema}

	gk = GroupKey{[]interface{}{3, 2}}
	expectedGroupKeys[gk.Hash()] = gk
	expectedGroupDfs[gk.Hash()] = Dataframe{df.Rows[6:8], df.Schema}

	gk = GroupKey{[]interface{}{4, 2}}
	expectedGroupKeys[gk.Hash()] = gk
	expectedGroupDfs[gk.Hash()] = Dataframe{[]Row{df.Rows[8]}, df.Schema}

	for k, gdf := range gb.Groups {

		expectedGroupKey := expectedGroupKeys[k.Hash()]
		if !(k.Hash() == expectedGroupKey.Hash()) {
			t.Error("Unexpected group.")
		}

		expectedGroupDf := expectedGroupDfs[k.Hash()]
		if !gdf.Equals(expectedGroupDf) {
			fmt.Println(k)
			gdf.Print()
			t.Error("Unexcpected group dataframe.")
		}
	}
}
