package main

import (
	"fmt"
	"parallel/dataframe"
)

func main() {

	raw_values := [][]interface{}{
		{1, 2, false, "a", 5, true},
		{1, 2, false, "b"},
		{1, 2, false, "c"},
		{2, 2, false, "a"},
		{2, 2, true},
		{2, 2, false, "c"},
		{3, 2, false, "zzzz"},
		{3, 2, false, 5, 5, 5.5, true},
		{4, true, true},
	}

	df := dataframe.CreateDataframe(raw_values)

	gb := df.GroupBy("column_0", "column_1")

	for k := range gb.Groups {

		fmt.Println(*k)
		gbDf := gb.Groups[k]
		gbDf.Print()

	}

}
