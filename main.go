package main

import (
	"fmt"
	"parallel/dataframe"
)

func main() {

	raw_values := [][]interface{}{}

	df := dataframe.CreateDataframe(raw_values)
	dfs := df.Split(3)

	fmt.Println(dfs)
	fmt.Println(len(dfs))
}
