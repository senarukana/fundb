package util

import (
	"fmt"
)

type StringSet map[string]bool

func NewStringSet() StringSet {
	return make(map[string]bool)
}

func NewStringSetFromStrings(strs []string) StringSet {
	s := NewStringSet()
	for _, str := range strs {
		s.Insert(str)
	}
	return s
}

func (s StringSet) Insert(str string) {
	if _, ok := s[str]; !ok {
		s[str] = true
	}
}

func (s StringSet) Exists(str string) bool {
	if _, ok := s[str]; ok {
		return true
	} else {
		return false
	}
}

func (s StringSet) Dup() StringSet {
	cs := NewStringSet()
	for str, _ := range s {
		cs.Insert(str)
	}
	return cs
}

func (s StringSet) ConvertToStrings() []string {
	res := make([]string, 0, len(s))
	for str, _ := range s {
		res = append(res, str)
	}
	return res
}

func (s StringSet) String() string {
	res := fmt.Sprint("StringSet: [ ")
	for str, _ := range s {
		res += fmt.Sprint(str + " ")
	}
	res += fmt.Sprintln("]")
	return res
}
