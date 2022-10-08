package dataframe

import (
	"parallel/core"
	"parallel/schema"
)

type DistictValues struct {
	ValuesKey []*core.Key
	Schema    schema.Schema
}

func (d *DistictValues) ValuesExist(vk core.Key) (bool, *core.Key) {

	for _, k := range d.ValuesKey {
		key := *k
		if vk.Hash() == key.Hash() {
			return true, k
		}
	}
	return false, nil

}

func (dv *DistictValues) Concat(dvStructs ...DistictValues) DistictValues {
	// Concatenates multiple GroupBy structs into one.

	for _, dvElement := range dvStructs {
		for _, k := range dvElement.ValuesKey {

			key := *k
			gkExists, _ := dv.ValuesExist(key)

			if !gkExists {
				dv.ValuesKey = append(dv.ValuesKey, &key)
			}
		}
	}

	return *dv
}

func (d *DistictValues) AsDataframe() Dataframe {

	rows := make([][]interface{}, len(d.ValuesKey))
	for idx, vk := range d.ValuesKey {
		valuesKey := *vk
		rows[idx] = valuesKey.Values
	}
	return CreateDataframe(rows, d.Schema)

}

func (df *Dataframe) distinctOperation(columnNames ...string) DistictValues {

	columnIndexes := make([]int, len(columnNames))
	columnsSchema := schema.Schema{}
	for idx, name := range columnNames {
		columnIndexes[idx] = df.Schema.ColumnIndex(name)
		columnsSchema.Columns = append(columnsSchema.Columns, df.Schema.Columns[columnIndexes[idx]])
	}

	distinctValues := DistictValues{}

	for _, row := range df.Rows {

		var v []interface{}
		for _, cIdx := range columnIndexes {
			v = append(v, row.Values[cIdx])
		}

		vk := core.Key{Values: v}
		vkExists, _ := distinctValues.ValuesExist(vk)
		if !(vkExists) {
			distinctValues.ValuesKey = append(distinctValues.ValuesKey, &vk)
		}

	}

	distinctValues.Schema = columnsSchema
	return distinctValues

}

func (df *Dataframe) Distinct(columnNames ...string) DistictValues {

	return distinctPool(df, columnNames...)

}

type distinctJob struct {
	df      Dataframe
	columns []string
}

func distinctWorker(jobs <-chan distinctJob, resluts chan<- DistictValues) {

	for dvJob := range jobs {
		resluts <- dvJob.df.distinctOperation(dvJob.columns...)
	}

}

func distinctPool(df *Dataframe, columnNames ...string) DistictValues {

	nWorkers := core.NumWorkers
	dfPackets := df.Split(nWorkers)

	// Initialise workers channels
	jobs := make(chan distinctJob, len(dfPackets))
	results := make(chan DistictValues, len(dfPackets))

	for i := 1; i <= nWorkers; i++ {
		go distinctWorker(jobs, results)
	}

	// Load sender channel
	for _, packetDf := range dfPackets {
		dvJob := distinctJob{df: packetDf, columns: columnNames}
		jobs <- dvJob
	}
	close(jobs)

	// Collect from receiver channel
	var dvPackets []DistictValues
	for i := 1; i <= len(dfPackets); i++ {
		dvPackets = append(dvPackets, <-results)
	}

	if len(dvPackets) == 1 {
		return dvPackets[0]
	}
	return dvPackets[0].Concat(dvPackets[1:]...)

}
