package util

import "fmt"

// check if an array contains a specified string
func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func Map(vs []string, f func(string) string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}

func ArrayToString[T any](a []T, delim string) string {
	var result string
	for index, item := range a {
		if index == 0 {
			result = result + fmt.Sprint(item)
		} else {
			result = result + delim + fmt.Sprint(item)
		}
	}
	return result
}
