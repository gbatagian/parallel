package main

import (
	"parallel/dataframe"
)

func main() {

	raw_values := [][]interface{}{
		{1, "a", false, 1.1},
		{"2.1", "b", true, 2.2, 1, 1.1, "a", true},
	}

	df := dataframe.CreateDatafeme(raw_values)

	df.Print()

}
