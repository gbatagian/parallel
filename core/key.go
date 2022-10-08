package core

import (
	"bytes"
	"crypto/sha256"
	"fmt"
)

type Key struct {
	Values []interface{}
}

func (k *Key) Hash() string {
	var buffer bytes.Buffer
	for _, v := range k.Values {
		s := fmt.Sprintf("%v", v)
		buffer.WriteString(s)
	}

	return fmt.Sprintf("%x", sha256.Sum256(buffer.Bytes()))
}
