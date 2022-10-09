package dataframe

import (
	"parallel/core"
)

type GroupBy struct {
	Groups map[*core.Key]Dataframe
}

func (g *GroupBy) GroupExists(gk core.Key) (bool, *core.Key) {

	for k := range g.Groups {
		key := *k
		if key.Hash() == gk.Hash() {
			return true, k
		}
	}
	return false, nil

}

func (gb *GroupBy) concat(gbStructs ...GroupBy) GroupBy {
	// Concatenates multiple GroupBy structs into one.

	for _, gbElement := range gbStructs {
		for k := range gbElement.Groups {

			key := *k
			gkExists, gkPointer := gb.GroupExists(key)

			if !gkExists {
				gb.Groups[&key] = gbElement.Groups[k]
			} else {
				gbGroupDf := gb.Groups[gkPointer]
				gbGroupDf.Rows = append(gbGroupDf.Rows, gbElement.Groups[k].Rows...)
				gb.Groups[gkPointer] = gbGroupDf
			}

		}
	}

	return *gb
}

func groupByOperation(df *Dataframe, columnNames ...string) GroupBy {

	columnIndexes := make([]int, len(columnNames))
	for idx, name := range columnNames {
		columnIndexes[idx] = df.Schema.ColumnIndex(name)
	}

	g := GroupBy{Groups: make(map[*core.Key]Dataframe)}

	for _, row := range df.Rows {

		var v []interface{}
		for _, cIdx := range columnIndexes {
			v = append(v, row.Values[cIdx])
		}

		gk := core.Key{Values: v}
		gkExists, gkPointer := g.GroupExists(gk)

		if !gkExists {
			g.Groups[&gk] = CreateDataframe([][]interface{}{row.Values}, df.Schema)
		} else {
			dfGroup := g.Groups[gkPointer]
			dfGroup.Rows = append(g.Groups[gkPointer].Rows, row)
			g.Groups[gkPointer] = dfGroup
		}
	}

	return g

}

func (df *Dataframe) GroupBy(columnNames ...string) GroupBy {

	opJ := ConcurrentOperationCore{
		df:      df,
		columns: columnNames,
		operation: func(df *Dataframe, columnNames ...string) interface{} {
			return groupByOperation(df, columnNames...)
		},
	}

	results := Pool(opJ)
	lenResults := len(results)

	if lenResults == 1 {
		return results[0].(GroupBy)
	}

	gbPackets := make([]GroupBy, lenResults)
	for idx, r := range results {
		if gb, ok := r.(GroupBy); ok {
			gbPackets[idx] = gb
		}
	}

	return gbPackets[0].concat(gbPackets[1:]...)

}
