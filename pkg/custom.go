package pkg

import (
	"fmt"
	"strings"
	"unicode"
)

func mapF(filename string, content string) []KeyValue {
	f := func(c rune) bool {
		return !(unicode.IsLetter(c) || unicode.IsNumber(c))
	}
	words := strings.FieldsFunc(content, f)

	kvs := make([]KeyValue, len(words))
	for _, word := range words {
		kvs = append(kvs, KeyValue{Key: word})
	}
	return kvs
}

func reduceF(key string, keyValues []KeyValue) string {
	return fmt.Sprintf("%d", len(keyValues))
}
