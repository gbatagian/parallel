package dataframe

import (
	"parallel/row"
	"sort"
)

type sortConstructor struct {
	rows                        *[]row.Row
	sortKeysPositions           []int
	reverse                     []bool
	currentSortKeyPositionIndex int
}

func (sc *sortConstructor) sortSliceWithMultipleIndexPositions(i, j int) bool {

	if sc.currentSortKeyPositionIndex > len(sc.sortKeysPositions)-1 {
		// To end up in here it means that two rows are equal based on the checks in all the specified key index positions
		sc.currentSortKeyPositionIndex = 0 // renormilise sort key index (to perform a fresh check in the next rows)
		return true
	}

	idx := sc.sortKeysPositions[sc.currentSortKeyPositionIndex]
	var con bool

	rows := *sc.rows
	switch v1 := rows[i].Values[idx].(type) {
	case int:
		if v1 < rows[j].Values[idx].(int) {
			con = true
		} else if v1 > rows[j].Values[idx].(int) {
			con = false
		} else {
			// rows are equal based on the current key index, so evaluate inequality condition in the next key index
			sc.currentSortKeyPositionIndex += 1
			return sc.sortSliceWithMultipleIndexPositions(i, j)
		}
	case int8:
		if v1 < rows[j].Values[idx].(int8) {
			con = true
		} else if v1 > rows[j].Values[idx].(int8) {
			con = false
		} else {
			// rows are equal based on the current key index, so evaluate inequality condition in the next key index
			sc.currentSortKeyPositionIndex += 1
			return sc.sortSliceWithMultipleIndexPositions(i, j)
		}
	case int16:
		if v1 < rows[j].Values[idx].(int16) {
			con = true
		} else if v1 > rows[j].Values[idx].(int16) {
			con = false
		} else {
			// rows are equal based on the current key index, so evaluate inequality condition in the next key index
			sc.currentSortKeyPositionIndex += 1
			return sc.sortSliceWithMultipleIndexPositions(i, j)
		}
	case int32:
		if v1 < rows[j].Values[idx].(int32) {
			con = true
		} else if v1 > rows[j].Values[idx].(int32) {
			con = false
		} else {
			// rows are equal based on the current key index, so evaluate inequality condition in the next key index
			sc.currentSortKeyPositionIndex += 1
			return sc.sortSliceWithMultipleIndexPositions(i, j)
		}
	case int64:
		if v1 < rows[j].Values[idx].(int64) {
			con = true
		} else if v1 > rows[j].Values[idx].(int64) {
			con = false
		} else {
			// rows are equal based on the current key index, so evaluate inequality condition in the next key index
			sc.currentSortKeyPositionIndex += 1
			return sc.sortSliceWithMultipleIndexPositions(i, j)
		}
	case float32:
		if v1 < rows[j].Values[idx].(float32) {
			con = true
		} else if v1 > rows[j].Values[idx].(float32) {
			con = false
		} else {
			// rows are equal based on the current key index, so evaluate inequality condition in the next key index
			sc.currentSortKeyPositionIndex += 1
			return sc.sortSliceWithMultipleIndexPositions(i, j)
		}
	case float64:
		if v1 < rows[j].Values[idx].(float64) {
			con = true
		} else if v1 > rows[j].Values[idx].(float64) {
			con = false
		} else {
			// rows are equal based on the current key index, so evaluate inequality condition in the next key index
			sc.currentSortKeyPositionIndex += 1
			return sc.sortSliceWithMultipleIndexPositions(i, j)
		}
	case string:
		if v1 < rows[j].Values[idx].(string) {
			con = true
		} else if v1 > rows[j].Values[idx].(string) {
			con = false
		} else {
			// rows are equal based on the current key index, so evaluate inequality condition in the next key index
			sc.currentSortKeyPositionIndex += 1
			return sc.sortSliceWithMultipleIndexPositions(i, j)
		}
	case bool:
		if v1 != rows[j].Values[idx].(bool) {
			con = v1
		} else {
			// rows are equal based on the current key index, so evaluate inequality condition in the next key index
			sc.currentSortKeyPositionIndex += 1
			return sc.sortSliceWithMultipleIndexPositions(i, j)
		}
	default:
		con = false
	}

	if sc.reverse[sc.currentSortKeyPositionIndex] {
		con = !con
	}

	if sc.currentSortKeyPositionIndex > 0 {
		// renormilise sort key index after each condition is resolved - before every return (to perform a fresh check in the next rows)
		sc.currentSortKeyPositionIndex = 0
	}

	return con

}

func (df *Dataframe) Sort(columnNames ...string) Dataframe {

	columnIndexes := make([]int, len(columnNames))
	sortColumnInReverseOrderMap := make([]bool, len(columnNames))

	for idx, name := range columnNames {
		if string(name[0]) == "~" {
			sortColumnInReverseOrderMap[idx] = true
			name = string(name[1:])
		}
		columnIndexes[idx] = df.Schema.ColumnIndexInSchema(name)

	}

	sc := sortConstructor{rows: &df.Rows, sortKeysPositions: columnIndexes, reverse: sortColumnInReverseOrderMap}
	sort.Slice(
		*sc.rows,
		sc.sortSliceWithMultipleIndexPositions,
	)

	return *df
}
