package dataframe

import (
	"parallel/core"
)

type GroupBy struct {
	Groups map[*core.ValuesKey]Dataframe
}

func (g *GroupBy) GroupExists(gk core.ValuesKey) (bool, *core.ValuesKey) {

	for k := range g.Groups {
		key := *k
		if key.Hash() == gk.Hash() {
			return true, k
		}
	}
	return false, nil

}

func (gb *GroupBy) Concat(gbStructs ...GroupBy) GroupBy {
	// Concatenates multiple GroupBy structs into one.

	for _, gbElement := range gbStructs {
		for k := range gbElement.Groups {

			key := *k
			gkExists, gkPointer := gb.GroupExists(key)

			if !gkExists {
				gb.Groups[&key] = gbElement.Groups[k]
			} else {
				gbFinalGroupDf := gb.Groups[gkPointer]
				gbFinalGroupDf.Rows = append(gbFinalGroupDf.Rows, gbElement.Groups[k].Rows...)
				gb.Groups[gkPointer] = gbFinalGroupDf
			}

		}
	}

	return *gb
}

func (df *Dataframe) groupByOperation(columnNames ...string) GroupBy {

	columnIndexes := make([]int, len(columnNames))
	for idx, name := range columnNames {
		columnIndexes[idx] = df.Schema.ColumnIndexInSchema(name)
	}

	g := GroupBy{Groups: make(map[*core.ValuesKey]Dataframe)}

	for _, row := range df.Rows {

		var v []interface{}
		for _, cIdx := range columnIndexes {
			v = append(v, row.Values[cIdx])
		}

		gk := core.ValuesKey{Values: v}
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

	return groupByPool(*df, columnNames...)

}

type groupByJob struct {
	df      Dataframe
	columns []string
}

func groupByWorker(jobs <-chan groupByJob, results chan<- GroupBy) {

	for gbJob := range jobs {
		results <- gbJob.df.groupByOperation(gbJob.columns...)
	}

}

func groupByPool(df Dataframe, columnNames ...string) GroupBy {

	nWorkers := core.NumWorkers
	dfPackets := df.Split(nWorkers)

	// Initialise workers channels
	jobs := make(chan groupByJob, len(dfPackets))
	results := make(chan GroupBy, len(dfPackets))

	for i := 1; i <= nWorkers; i++ {
		go groupByWorker(jobs, results)
	}

	// Load sender channel
	for _, packetDf := range dfPackets {
		gbJob := groupByJob{df: packetDf, columns: columnNames}
		jobs <- gbJob
	}
	close(jobs)

	// Collect from receiver channel
	var gbPackets []GroupBy
	for i := 1; i <= len(dfPackets); i++ {
		gbPackets = append(gbPackets, <-results)
	}

	if len(gbPackets) == 1 {
		return gbPackets[0]
	}
	return gbPackets[0].Concat(gbPackets[1:]...)

}
