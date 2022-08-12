package main

import (
	"fmt"
	"math"
	"parallel/dataframe"
	"parallel/types"
)

func main() {

	raw_values := [][]interface{}{
		{2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a"},
		{2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a"},
		{2.1, "2022-06-01 19:58:30.991242+00", "b", true, 2.2, 1, "a"},
		{math.NaN(), "2022-06-01 19:58:30.991242+00", "b", true, 2.2, math.NaN(), "a"},
	}

	schema := dataframe.Schema{
		Columns: []dataframe.Column{
			{
				Type: types.Float,
				Name: "a",
			},
			{
				Type: types.String,
				Name: "b",
			},
			{
				Type: types.String,
				Name: "c",
			},
			{
				Type: types.Bool,
				Name: "d",
			},
			{
				Type: types.Float,
				Name: "e",
			},
			{
				Type: types.Int,
				Name: "f",
			},
			{
				Type: types.String,
				Name: "g",
			},
		},
	}
	df := dataframe.CreateDataframe(raw_values, schema)

	df.Print()

	fmt.Println(df.ColumnTypes())

}
