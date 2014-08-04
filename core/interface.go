package core

type DBEngine interface {
	Query(sql string) *Response
}
