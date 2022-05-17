package dataframe

import (
	"parallel/types"
)

// Structure representing the definition of a datafrane column.
type Column struct {
	Name string
	Type types.DataType
}
