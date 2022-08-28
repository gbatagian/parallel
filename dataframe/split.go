package dataframe

import (
	"math"
)

func (df *Dataframe) Split(numPackets int) []Dataframe {

	var numRows int = len(df.Rows)
	var rowsPerPacket int = int(math.Ceil(float64(numRows) / float64(numPackets)))
	var dfPackets []Dataframe

	for idx := 0; idx < numRows; idx = idx + rowsPerPacket {
		dfPackets = append(dfPackets, df.RSlice(idx, idx+rowsPerPacket))
	}

	return dfPackets

}
