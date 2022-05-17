package main

import (
	"fmt"
	"parallel/dataframe"
	"parallel/types"
)

func main() {

	schema := dataframe.Schema{
		Columns: []dataframe.Column{
			{
				Name: "var1",
				Type: types.String,
			},
			{
				Name: "var2",
				Type: types.Int,
			},
		},
	}
	raw_data_1 := []interface{}{"A", 1}
	row_1 := dataframe.CreateRow(raw_data_1, schema)

	fmt.Println(row_1)

	fmt.Println("---------------")

	row_2 := dataframe.CreateRow(
		[]interface{}{"A", 1, 14.444, false},
	)

	fmt.Println(row_2)

	fmt.Println("---------------")

	row_3 := dataframe.CreateRow(
		[]interface{}{"A", 1, 14.444, false},
		[]string{"String_Column", "Integer_Column", "Float_Column", "Boolean_Column"},
	)

	fmt.Println(row_3)

}
