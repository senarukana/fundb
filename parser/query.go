package parser

import (
	"fmt"

	"github.com/golang/glog"
)

type QueryType int

const (
	INVALID QueryType = iota
	QUERY_SELECT
	QUERY_DELETE
	QUERY_INSERT
	QUERY_UPDATE
	QUERY_SCHEMA_TABLE_CREATE
)

func (self QueryType) String() string {
	switch self {
	case QUERY_SELECT:
		return "QUERY_SELECT"
	case QUERY_DELETE:
		return "QUERY_DELETE"
	case QUERY_INSERT:
		return "QUERY_INSERT"
	case QUERY_UPDATE:
		return "QUERY_UPDATE"
	case QUERY_SCHEMA_TABLE_CREATE:
		return "QUERY_SCHEMA_TABLE_CREATE"
	default:
		return "INVALID"
	}
}

type ParserError struct {
	Message string
}

func NewParserError(format string, args ...interface{}) ParserError {
	return ParserError{fmt.Sprintf(format, args...)}
}

func (err ParserError) Error() string {
	return err.Message
}

type Query interface {
	Validate() error
	GetSplitIds(splitField string) (ids []int64)
	GetTableName() string
}

type InsertQuery struct {
	Table string
	*ColumnFields
	*ValueList
}

func (self *InsertQuery) Validate() error {
	if len(self.Values[0].Items) != len(self.Fields) {
		return fmt.Errorf("syntax error: Incompatible fields(%d) and values(%d)",
			len(self.Values[0].Items), len(self.Fields))
	}

	var paramCount = -1
	for valueIndex, valueItems := range self.Values {
		if paramCount == -1 {
			paramCount = len(valueItems.Items)
		}
		if paramCount != len(valueItems.Items) {
			return fmt.Errorf("syntax error: Incompatible value paramters in %d, paremter num is %d, exptected %d",
				valueIndex, len(valueItems.Items), paramCount)
		}

	}
	return nil
}

func (self *InsertQuery) GetSplitIds(splitField string) (ids []int64) {
	idx := -1
	for i, field := range self.Fields {
		if field == splitField {
			idx = i
			break
		}
	}

	if idx == -1 {
		glog.Fatalf("NOT FOUND SPLIT FIELD %s IN INSERT QUERY", splitField)
	}
	for _, values := range self.Values {
		ids = append(ids, values.Items[idx].GetVal().GetIntVal())
	}
	return ids
}

func (self *InsertQuery) GetTableName() string {
	return self.Table
}

type SelectQuery struct {
	Distinct bool
	*SelectExpression
	*TableExpression
	*OrderByList
	Limit int
}

type DeleteQuery struct {
	*TableExpression
}

type CreateTableQuery struct {
	Name string
	Type TableIdType
}

type QuerySpec struct {
	Type  QueryType
	Query Query
}

func ParseQuery(query string) (*Query, error) {
	lex := NewLex(query)
	if FunDBParse(lex) != 0 {
		return nil, NewParserError(lex.LastError)
	}
	parsedQuery := ParsedQuery
	if err := parsedQuery.Validate(); err != nil {
		return nil, err
	}
	return parsedQuery, nil
}
