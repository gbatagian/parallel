package core

import (
	"bytes"
	"crypto/sha256"
	"fmt"
)

type ValuesKey struct {
	Values []interface{}
}

func (k *ValuesKey) Hash() string {
	var buffer bytes.Buffer
	for _, v := range k.Values {
		s := fmt.Sprintf("%v", v)
		buffer.WriteString(s)
	}

	return fmt.Sprintf("%x", sha256.Sum256(buffer.Bytes()))
}
