package dataframe

import (
	"bytes"
	"crypto/sha256"
	"fmt"
)

type GroupKey struct {
	Key []interface{}
}

func (k *GroupKey) Hash() string {
	var buffer bytes.Buffer
	for _, v := range k.Key {
		s := fmt.Sprintf("%v", v)
		buffer.WriteString(s)
	}

	return fmt.Sprintf("%x", sha256.Sum256(buffer.Bytes()))
}

type GroupBy struct {
	Groups map[*GroupKey]Dataframe
}

func (g *GroupBy) GroupExists(gk GroupKey) (bool, *GroupKey) {

	for k := range g.Groups {
		key := *k
		if key.Hash() == gk.Hash() {
			return true, k
		}
	}
	return false, nil

}

func (df *Dataframe) GroupBy(columnNames ...string) GroupBy {

	columnIndexes := make([]int, len(columnNames))
	for idx, name := range columnNames {
		columnIndexes[idx] = df.Schema.ColumnIndexInSchema(name)
	}

	g := GroupBy{Groups: make(map[*GroupKey]Dataframe)}

	for _, row := range df.Rows {

		var key []interface{}
		for _, cIdx := range columnIndexes {
			key = append(key, row.Values[cIdx])
		}

		gk := GroupKey{key}
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
