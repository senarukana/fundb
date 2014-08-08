package parser

import (
	"errors"
	"fmt"
	"math"

	"github.com/senarukana/fundb/protocol"
	"github.com/senarukana/fundb/util"
)

const (
	MaximumRange int64 = math.MaxInt64
)

var (
	ErrNotIdField = errors.New("not it field")
)

func (self *WhereExpression) getIdFromBetween() (int64, int64, error) {
	field := self.Left.(string)
	if field != "_id" {
		return 0, MaximumRange, ErrNotIdField
	}
	betweenExpr := self.Right.(*BetweenExpression)
	if betweenExpr.Left.Type != SCLAR_LITERAL || betweenExpr.Right.Type != SCLAR_LITERAL {
		panic("NOT SUPPORTED SCALAR TYPE")
	}
	leftField := betweenExpr.Left.Val.(LiteralNode)
	rightField := betweenExpr.Right.Val.(LiteralNode)

	if leftField.GetType() != protocol.INT || rightField.GetType() != protocol.INT {
		return MaximumRange, MaximumRange, fmt.Errorf("Invalid _id type %v, exptected INT", leftField.GetType())
	}
	idStart := leftField.GetVal().GetIntVal()
	idEnd := rightField.GetVal().GetIntVal()
	if idStart > idEnd {
		return MaximumRange, MaximumRange, fmt.Errorf("Range of Between is invalid, %d is bigger than %d", idStart, idEnd)
	}
	return idStart, idEnd - 1, nil
}

func (self *WhereExpression) getIdFromComparison() (int64, int64, error) {
	fieldName := self.Left.(string)
	if fieldName != "_id" {
		return 0, MaximumRange, ErrNotIdField
	}
	rightScalar := self.Right.(*Scalar)
	if rightScalar.Type != SCLAR_LITERAL {
		panic("NOT SUPPORTED SCALAR TYPE")
	}
	rightNode := rightScalar.Val.(LiteralNode)
	if rightNode.GetType() != protocol.INT {
		return 0, MaximumRange, fmt.Errorf("Invalid _id type %v, exptected INT", rightNode.GetType())
	}
	val := rightNode.GetVal().GetIntVal()
	switch ComparisonMap[self.Token.Src] {
	case EQUAL:
		return val, val, nil
	case GREATER:
		return val + 1, MaximumRange, nil
	case GREATEREQ:
		return val, MaximumRange, nil
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
		return nil, 0, MaximumRange, nil
	}
	switch condition.Type {
	case WHERE_BETWEEN:
		idStart, idEnd, err := condition.getIdFromBetween()
		if err == ErrNotIdField {
			return condition, idStart, idEnd, nil
		} else {
			return nil, idStart, idEnd, err
		}
	case WHERE_COMPARISON:
		idStart, idEnd, err := condition.getIdFromComparison()
		if err == ErrNotIdField {
			return condition, idStart, idEnd, nil
		} else {
			return nil, idStart, idEnd, err
		}
		return nil, idStart, idEnd, err
	case WHERE_AND:
		leftCondition, leftStart, leftEnd, err := GetIdCondition(condition.Left.(*WhereExpression))
		if err != nil {
			return nil, MaximumRange, MaximumRange, err
		}
		rightCondition, rightStart, rightEnd, err := GetIdCondition(condition.Right.(*WhereExpression))
		if err != nil {
			return nil, MaximumRange, MaximumRange, err
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
		if leftStart == MaximumRange && rightStart == MaximumRange {
			idStart = 0
		} else if leftStart != MaximumRange && rightStart == MaximumRange {
			idStart = leftStart
		} else if rightStart != MaximumRange && leftStart == MaximumRange {
			idStart = rightStart
		} else {
			if leftStart > rightStart {
				idStart = leftStart
			} else {
				idStart = rightStart
			}
		}

		if leftEnd == MaximumRange && rightEnd == MaximumRange {
			idEnd = MaximumRange
		} else if leftEnd != MaximumRange && rightEnd == MaximumRange {
			idEnd = leftEnd
		} else if rightEnd != MaximumRange && leftEnd == MaximumRange {
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

func (self *WhereExpression) getConditionFields(columnSet util.StringSet) {
	switch self.Type {
	case WHERE_AND:
		self.Left.(*WhereExpression).getConditionFields(columnSet)
		self.Right.(*WhereExpression).getConditionFields(columnSet)
	case WHERE_COMPARISON:
		fieldName := self.Left.(string)
		columnSet.Insert(fieldName)
	case WHERE_BETWEEN:
		fieldName := self.Left.(string)
		columnSet.Insert(fieldName)
	default:
		panic(fmt.Sprintf("UNKNOWN WHERE TYPE %d", self.Type))
	}
}

func (self *SelectQuery) getSelectFields(columnSet util.StringSet) {
	for _, scalar := range self.ScalarList.ScalarList {
		switch scalar.Type {
		case SCALAR_IDENT:
			ident := scalar.Val.(string)
			columnSet.Insert(ident)
		default:
			panic("SCALAR TYPE NOT SUPPORTED")
		}
	}
}

func (self *WhereExpression) GetConditionFields() []string {
	columnSet := util.NewStringSet()
	self.getConditionFields(columnSet)
	return columnSet.ConvertToStrings()
}

func (self *SelectQuery) GetSelectAndConditionFields() []string {
	columnSet := util.NewStringSet()
	if self.WhereExpression != nil {
		self.WhereExpression.getConditionFields(columnSet)
	}
	self.getSelectFields(columnSet)
	return columnSet.ConvertToStrings()
}

func (self *SelectQuery) GetSelectFields() []string {
	columnSet := util.NewStringSet()
	self.getSelectFields(columnSet)
	return columnSet.ConvertToStrings()
}
