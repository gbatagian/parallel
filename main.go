package main

import (
	"parallel/dataframe"
)

func main() {

	raw_values := [][]interface{}{
		{1, 2, false, "a"},
		{1, 2, false, "b"},
		{1, 2, false, "c"},
		{2, 2, false, "d"},
		{2, 2, true},
		{2, 2, false, "e"},
		{1, 2, false, "f"},
		{1, 2, false, "g"},
		{1, 2, true},
	}

	d := dataframe.CreateDataframe(raw_values)

	sd := d.Sort("column_0", "~column_2", "column_3")

	sd.Print(100)
}
