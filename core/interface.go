package core

type Store interface {
	IsLocal() bool
	Query(sql string) *Response
}
