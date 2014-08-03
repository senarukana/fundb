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

type Query struct {
	kind     QueryType
	query    string
	queryAST interface{}
}

func ParseQuery(query string) (*Query, error) {
	lex := NewLex(query)
	if FunDBParse(lex) != 0 {
		return nil, NewParserError(lex.LastError)
	}
	parsedQuery := ParsedQuery
	parsedQuery.query = query
	return parsedQuery, nil
}
