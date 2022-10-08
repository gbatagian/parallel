package dataframe

import (
	"parallel/core"
	"parallel/schema"
)

type DistictValues struct {
	ValuesKey map[*core.Key]int
	Schema    schema.Schema
}

func (d *DistictValues) ValuesExist(vk core.Key) (bool, *core.Key) {

	for k := range d.ValuesKey {
		key := *k
		if vk.Hash() == key.Hash() {
			return true, k
		}
	}
	return false, nil

}

func (d *DistictValues) AsDataframe() Dataframe {

	rows := make([][]interface{}, len(d.ValuesKey))
	for vk, idx := range d.ValuesKey {
		valuesKey := *vk
		rows[idx] = valuesKey.Values
	}
	return CreateDataframe(rows, d.Schema)

}

func (df *Dataframe) Distinct(columnNames ...string) DistictValues {

	columnIndexes := make([]int, len(columnNames))
	columnsSchema := schema.Schema{}
	for idx, name := range columnNames {
		columnIndexes[idx] = df.Schema.ColumnIndex(name)
		columnsSchema.Columns = append(columnsSchema.Columns, df.Schema.Columns[columnIndexes[idx]])
	}

	distinctValues := DistictValues{ValuesKey: make(map[*core.Key]int)}

	i := 0
	for _, row := range df.Rows {

		var v []interface{}
		for _, cIdx := range columnIndexes {
			v = append(v, row.Values[cIdx])
		}

		vk := core.Key{Values: v}
		vkExists, _ := distinctValues.ValuesExist(vk)
		if !(vkExists) {
			distinctValues.ValuesKey[&vk] = i
			i++
		}

	}

	distinctValues.Schema = columnsSchema
	return distinctValues

}
