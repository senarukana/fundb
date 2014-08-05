package util

type StringSet map[string]bool

func NewStringSet() StringSet {
	return make(map[string]bool)
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

func (s StringSet) ConvertToStrings() []string {
	res := make([]string, 0, len(s))
	for s, _ := range s {
		res = append(res, s)
	}
	return res
}
