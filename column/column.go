package column

import (
	"parallel/types"
)

type Column struct {
	Name string
	Type types.DataType
}

func (c1 *Column) Equals(c2 Column) bool {
	if c1.Name == c2.Name && c1.Type == c2.Type {
		return true
	}
	return false
}
