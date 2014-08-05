package core

import (
	"fmt"
	"math"

	"github.com/senarukana/fundb/parser"
	"github.com/senarukana/fundb/protocol"
	"github.com/senarukana/fundb/util"
)

const (
	InvalidInt int64 = math.MaxInt64
)

func getIdFromBetween(condition *parser.WhereExpression) (int64, int64, error) {
	field := condition.Left.(string)
	if field != "_id" {
		return InvalidInt, InvalidInt, nil
	}
	betweenExpr := condition.Right.(*parser.BetweenExpression)
	if betweenExpr.Left.Type != parser.SCLAR_LITERAL || betweenExpr.Right.Type != parser.SCLAR_LITERAL {
		panic("NOT SUPPORTED SCALAR TYPE")
	}
	leftField := betweenExpr.Left.Val.(*protocol.FieldValue)
	rightField := betweenExpr.Right.Val.(*protocol.FieldValue)

	if leftField.IntVal == nil || rightField.IntVal == nil {
		return InvalidInt, InvalidInt, fmt.Errorf("Parameter in Between should only be integer")
	}
	idStart := leftField.GetIntVal()
	idEnd := rightField.GetIntVal()
	if idStart > idEnd {
		return InvalidInt, InvalidInt, fmt.Errorf("Range of Between is invalid, %d is bigger than %d", idStart, idEnd)
	}
	return idStart, idEnd, nil
}

func getIdFromComparison(condition *parser.WhereExpression) (int64, int64, error) {
	fieldName := condition.Left.(string)
	if fieldName != "_id" {
		return InvalidInt, InvalidInt, nil
	}
	rightScalar := condition.Right.(*parser.Scalar)
	if rightScalar.Type != parser.SCLAR_LITERAL {
		panic("NOT SUPPORTED SCALAR TYPE")
	}
	rightField := rightScalar.Val.(*protocol.FieldValue)

	val := rightField.GetIntVal()
	switch parser.ComparisonMap[condition.Token.Src] {
	case parser.EQUAL:
		return val, val, nil
	case parser.GREATER:
		return val + 1, InvalidInt, nil
	case parser.GREATEREQ:
		return val, InvalidInt, nil
	case parser.SMALLER:
		return InvalidInt, val - 1, nil
	case parser.SMALLEREQ:
		return InvalidInt, val, nil
	default:
		panic("Invalid token type")
	}
	panic("shouldn't go here")
}

// parse the start time or end time from the where conditions and return the new condition
// without the time clauses, or nil if there are no where conditions left
func getIdCondition(condition *parser.WhereExpression) (*parser.WhereExpression, int64, int64, error) {
	switch condition.Type {
	case parser.WHERE_BETWEEN:
		idStart, idEnd, err := getIdFromBetween(condition)
		return nil, idStart, idEnd, err
	case parser.WHERE_COMPARISON:
		idStart, idEnd, err := getIdFromComparison(condition)
		return nil, idStart, idEnd, err
	case parser.WHERE_AND:
		leftCondition, leftStart, leftEnd, err := getIdCondition(condition.Left.(*parser.WhereExpression))
		if err != nil {
			return nil, InvalidInt, InvalidInt, err
		}
		rightCondition, rightStart, rightEnd, err := getIdCondition(condition.Right.(*parser.WhereExpression))
		if err != nil {
			return nil, InvalidInt, InvalidInt, err
		}
		newCondition := condition
		if leftCondition == nil {
			newCondition = rightCondition
		} else if rightCondition == nil {
			newCondition = leftCondition
		} else {
			newCondition.Left = leftCondition
			newCondition.Right = rightCondition
		}
		var idStart, idEnd int64
		if leftStart == InvalidInt && rightStart == InvalidInt {
			idStart = 0
		} else if leftStart != InvalidInt && rightStart == InvalidInt {
			idStart = leftStart
		} else if rightStart != InvalidInt && leftStart == InvalidInt {
			idStart = rightStart
		} else {
			if leftStart > rightStart {
				idStart = leftStart
			} else {
				idStart = rightStart
			}
		}

		if leftEnd == InvalidInt && rightEnd == InvalidInt {
			idEnd = InvalidInt
		} else if leftEnd != InvalidInt && rightEnd == InvalidInt {
			idEnd = leftEnd
		} else if rightEnd != InvalidInt && leftEnd == InvalidInt {
			idEnd = rightEnd
		} else {
			if leftEnd < rightEnd {
				idEnd = leftEnd
			} else {
				idEnd = rightEnd
			}
		}

		return newCondition, idStart, idEnd, nil
	}
	panic("shouldn't go here")
}

func getSelectionColumns(scalarList []*parser.Scalar, columnSet util.StringSet) {
	for _, scalar := range scalarList {
		switch scalar.Type {
		case parser.SCALAR_IDENT:
			ident := scalar.Val.(string)
			columnSet.Insert(ident)
		default:
			panic("SCALAR TYPE NOT SUPPORTED")
		}
	}
}

func getWhereColumns(condition *parser.WhereExpression, columnSet util.StringSet) {
	switch condition.Type {
	case parser.WHERE_AND:
		getWhereColumns(condition.Left.(*parser.WhereExpression), columnSet)
		getWhereColumns(condition.Right.(*parser.WhereExpression), columnSet)
	case parser.WHERE_COMPARISON:
		fieldName := condition.Left.(string)
		columnSet.Insert(fieldName)
	case parser.WHERE_BETWEEN:
		fieldName := condition.Left.(string)
		columnSet.Insert(fieldName)
	default:
		panic(fmt.Sprintf("UNKNOWN WHERE TYPE %d", condition.Type))
	}
}

func getFetchColumns(query *parser.SelectQuery) []string {
	columnSet := util.NewStringSet()
	getSelectionColumns(query.ScalarList.ScalarList, columnSet)
	getWhereColumns(query.WhereExpression, columnSet)
	return columnSet.ConvertToStrings()
}
