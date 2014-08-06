package parser

import (
	"errors"
	"fmt"
	"math"

	"github.com/senarukana/fundb/protocol"
	"github.com/senarukana/fundb/util"
)

const (
	InvalidRange int64 = math.MaxInt64
)

var (
	ErrNotIdField = errors.New("not it field")
)

func getIdFromBetween(condition *WhereExpression) (int64, int64, error) {
	field := condition.Left.(string)
	if field != "_id" {
		return 0, InvalidRange, ErrNotIdField
	}
	betweenExpr := condition.Right.(*BetweenExpression)
	if betweenExpr.Left.Type != SCLAR_LITERAL || betweenExpr.Right.Type != SCLAR_LITERAL {
		panic("NOT SUPPORTED SCALAR TYPE")
	}
	leftField := betweenExpr.Left.Val.(LiteralNode)
	rightField := betweenExpr.Right.Val.(LiteralNode)

	if leftField.GetType() != protocol.INT || rightField.GetType() != protocol.INT {
		return InvalidRange, InvalidRange, fmt.Errorf("Invalid _id type %v, exptected INT", leftField.GetType())
	}
	idStart := leftField.GetVal().GetIntVal()
	idEnd := rightField.GetVal().GetIntVal()
	if idStart > idEnd {
		return InvalidRange, InvalidRange, fmt.Errorf("Range of Between is invalid, %d is bigger than %d", idStart, idEnd)
	}
	return idStart, idEnd - 1, nil
}

func getIdFromComparison(condition *WhereExpression) (int64, int64, error) {
	fieldName := condition.Left.(string)
	if fieldName != "_id" {
		return 0, InvalidRange, ErrNotIdField
	}
	rightScalar := condition.Right.(*Scalar)
	if rightScalar.Type != SCLAR_LITERAL {
		panic("NOT SUPPORTED SCALAR TYPE")
	}
	rightNode := rightScalar.Val.(LiteralNode)
	if rightNode.GetType() != protocol.INT {
		return 0, InvalidRange, fmt.Errorf("Invalid _id type %v, exptected INT", rightNode.GetType())
	}
	val := rightNode.GetVal().GetIntVal()
	switch ComparisonMap[condition.Token.Src] {
	case EQUAL:
		return val, val, nil
	case GREATER:
		return val + 1, InvalidRange, nil
	case GREATEREQ:
		return val, InvalidRange, nil
	case SMALLER:
		return 0, val - 1, nil
	case SMALLEREQ:
		return 0, val, nil
	default:
		panic("Invalid token type")
	}
	panic("shouldn't go here")
}

func GetIdCondition(condition *WhereExpression) (*WhereExpression, int64, int64, error) {
	if condition == nil {
		return nil, 0, InvalidRange, nil
	}
	switch condition.Type {
	case WHERE_BETWEEN:
		idStart, idEnd, err := getIdFromBetween(condition)
		if err == ErrNotIdField {
			return condition, idStart, idEnd, nil
		} else {
			return nil, idStart, idEnd, err
		}
	case WHERE_COMPARISON:
		idStart, idEnd, err := getIdFromComparison(condition)
		if err == ErrNotIdField {
			return condition, idStart, idEnd, nil
		} else {
			return nil, idStart, idEnd, err
		}
		return nil, idStart, idEnd, err
	case WHERE_AND:
		leftCondition, leftStart, leftEnd, err := GetIdCondition(condition.Left.(*WhereExpression))
		if err != nil {
			return nil, InvalidRange, InvalidRange, err
		}
		rightCondition, rightStart, rightEnd, err := GetIdCondition(condition.Right.(*WhereExpression))
		if err != nil {
			return nil, InvalidRange, InvalidRange, err
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
		if leftStart == InvalidRange && rightStart == InvalidRange {
			idStart = 0
		} else if leftStart != InvalidRange && rightStart == InvalidRange {
			idStart = leftStart
		} else if rightStart != InvalidRange && leftStart == InvalidRange {
			idStart = rightStart
		} else {
			if leftStart > rightStart {
				idStart = leftStart
			} else {
				idStart = rightStart
			}
		}

		if leftEnd == InvalidRange && rightEnd == InvalidRange {
			idEnd = InvalidRange
		} else if leftEnd != InvalidRange && rightEnd == InvalidRange {
			idEnd = leftEnd
		} else if rightEnd != InvalidRange && leftEnd == InvalidRange {
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

func getSelectionFields(scalarList []*Scalar, columnSet util.StringSet) {
	for _, scalar := range scalarList {
		switch scalar.Type {
		case SCALAR_IDENT:
			ident := scalar.Val.(string)
			columnSet.Insert(ident)
		default:
			panic("SCALAR TYPE NOT SUPPORTED")
		}
	}
}

func getWhereFields(condition *WhereExpression, columnSet util.StringSet) {
	if condition == nil {
		return
	}
	switch condition.Type {
	case WHERE_AND:
		getWhereFields(condition.Left.(*WhereExpression), columnSet)
		getWhereFields(condition.Right.(*WhereExpression), columnSet)
	case WHERE_COMPARISON:
		fieldName := condition.Left.(string)
		columnSet.Insert(fieldName)
	case WHERE_BETWEEN:
		fieldName := condition.Left.(string)
		columnSet.Insert(fieldName)
	default:
		panic(fmt.Sprintf("UNKNOWN WHERE TYPE %d", condition.Type))
	}
}

func GetWhereFields(condition *WhereExpression) util.StringSet {
	columnSet := util.NewStringSet()
	getWhereFields(condition, columnSet)
	return columnSet
}

func GetAllFields(query *SelectQuery) (util.StringSet, []string) {
	columnSet := util.NewStringSet()
	getSelectionFields(query.ScalarList.ScalarList, columnSet)
	selectColumns := columnSet.Dup()
	getWhereFields(query.WhereExpression, columnSet)
	return selectColumns, columnSet.ConvertToStrings()
}
