package parser

import (
// "fmt"
)

type ScalarType int

const (
	SCALAR_IDENT ScalarType = iota
	SCLAR_LITERAL
)

type WhereType int

const (
	WHERE_AND WhereType = iota
	WHERE_COMPARISON
	WHERE_BETWEEN
)

type TableIdType int

const (
	TABLE_ID_RANDOM TableIdType = iota
	TABLE_ID_INCREMENT
)

type WhereExpression struct {
	Left  interface{}
	Right interface{}
	Type  WhereType
	Token Token
}

type TableExpression struct {
	*FromExpression
	*WhereExpression
	*OrderByList
}

type BetweenExpression struct {
	Left  *Scalar
	Right *Scalar
}

type SelectExpression struct {
	IsStar bool
	*ScalarList
}

type ScalarList struct {
	ScalarList []*Scalar
}

type Scalar struct {
	Type ScalarType
	Val  interface{}
}

type FromExpression struct {
	Table string
}

type OrderByList struct {
	OrderBys []*OrderBy
}

type OrderBy struct {
	Field string
	Order int
}

type ValueItems struct {
	Items []LiteralNode
}

type ValueList struct {
	Values []*ValueItems
}

type ColumnFields struct {
	Fields []string
}

func NewBetweenExpression(token Token, field string, left, right *Scalar) *WhereExpression {
	return &WhereExpression{
		Type:  WHERE_BETWEEN,
		Left:  field,
		Token: token,
		Right: &BetweenExpression{
			Left:  left,
			Right: right,
		},
	}
}

func NewScalarList(scalar *Scalar) *ScalarList {
	return &ScalarList{
		ScalarList: []*Scalar{scalar},
	}
}

func ScalarListAppend(scalarList *ScalarList, scalar *Scalar) *ScalarList {
	if scalarList == nil {
		return NewScalarList(scalar)
	}
	scalarList.ScalarList = append(scalarList.ScalarList, scalar)
	return scalarList
}

func NewOrderByList(order *OrderBy) *OrderByList {
	return &OrderByList{
		OrderBys: []*OrderBy{order},
	}
}

func OrderByListAppend(orderList *OrderByList, order *OrderBy) *OrderByList {
	if orderList == nil {
		return NewOrderByList(order)
	}
	orderList.OrderBys = append(orderList.OrderBys, order)
	return orderList
}

func NewColumnField(field string) *ColumnFields {
	return &ColumnFields{
		Fields: []string{field},
	}
}

func ColumnFieldsAppend(columnFields *ColumnFields, field string) *ColumnFields {
	if columnFields == nil {
		return NewColumnField(field)
	}
	columnFields.Fields = append(columnFields.Fields, field)
	return columnFields
}

func NewValueItem(item LiteralNode) *ValueItems {
	return &ValueItems{
		Items: []LiteralNode{item},
	}
}

func ValueItemAppend(valueItems *ValueItems, item LiteralNode) *ValueItems {
	if valueItems == nil {
		return NewValueItem(item)
	}
	valueItems.Items = append(valueItems.Items, item)
	return valueItems
}

func NewValueList(items *ValueItems) *ValueList {
	return &ValueList{
		Values: []*ValueItems{items},
	}
}

func ValueListAppend(valueList *ValueList, items *ValueItems) *ValueList {
	if valueList == nil {
		return NewValueList(items)
	}
	valueList.Values = append(valueList.Values, items)
	return valueList
}
