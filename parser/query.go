package parser

import (
	"fmt"
)

type QueryType int

const (
	INVALID QueryType = iota
	QUERY_SELECT
	QUERY_DELETE
	QUERY_INSERT
	QUERY_UPDATE
)

type ParserError struct {
	Message string
}

func NewParserError(format string, args ...interface{}) ParserError {
	return ParserError{fmt.Sprintf(format, args...)}
}

func (err ParserError) Error() string {
	return err.Message
}

type InsertQuery struct {
	Table     string
	Fields    *ColumnFields
	ValueList *ValueList
}

type Query struct {
	Type  QueryType
	Query interface{}
}

func ParseQuery(query string) (*Query, error) {
	lex := NewLex(query)
	if FunDBParse(lex) != 0 {
		return nil, NewParserError(lex.LastError)
	}
	parsedQuery := ParsedQuery
	return parsedQuery, nil
}
