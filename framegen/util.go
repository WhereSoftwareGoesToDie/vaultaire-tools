package main

import (
	"math/rand"
)

func randomAlphaByte() byte {
	b := rand.Intn(26) + 97
	return byte(b)
}

func genAlphaString(n int) string {
	data := make([]byte, n)
	for i, _ := range data {
		data[i] = randomAlphaByte()
	}
	return string(data[:n])
}

func genTestPattern() string {
	l := rand.Intn(20)
	return genAlphaString(l)
}
