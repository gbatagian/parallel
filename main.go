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
				Type: types.String,
			},
		},
	}
	raw_data := []interface{}{true, 1}

	row := dataframe.CreateRow(raw_data, schema)

	fmt.Println(row)

}
