package main

import "parallel/dataframe"

func main() {

	rawValues := [][]interface{}{
		{1, 2.1},
		{2, 2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, 1, true},
		{3, 2.1, "2022-06-01 19:58:30.991242+00", "b", true},
		{4},
		{5, 2.1, 2},
		{6, 2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a", "true"},
		{7, 2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, 1, true},
	}

	df := dataframe.CreateDataframe(&rawValues)

	df.Print()

}
