package main

import (
	"parallel/dataframe"
)

func main() {

	raw_values := [][]interface{}{
		{1},
		{"2.1", "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, 1.1, "a", true},
		{"2.1", "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, 1.1, "a", true},
		{"2.1", "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, 1.1, "a", true},
		{"2.1", "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, 1.1, "a", true},
		{"2.1", "2022-06-01 19:58:30.991242+00", "b", true},
		{5},
		{true, 2},
		{"2.1", "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, 1.1, "a", "true"},
		{"2.1", "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, 1.1, "a", true},
		{"2.1", "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, 1.1, "a", true},
	}

	df := dataframe.CreateDatafeme(raw_values)

	df.Print(len(df.Rows))

}
