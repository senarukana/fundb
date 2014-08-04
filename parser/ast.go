package parser

import (
	"fmt"
	"strconv"

	"github.com/senarukana/fundb/protocol"
)

type ScalarType int

const (
	SCALAR_IDENT ScalarType = iota
	SCLAR_LITERAL
)

type WhereExpression struct {
	Left   interface{}
	Right  interface{}
	IsBool bool
	Token  string
}

type TableExpression struct {
	*FromExpression
	*WhereExpression
	*OrderByList
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
	Items []*protocol.FieldValue
}

type ValueList struct {
	Values []*ValueItems
}

type ColumnFields struct {
	Fields []string
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

func NewFieldValue(fieldType protocol.FieldType, src string) *protocol.FieldValue {
	field := &protocol.FieldValue{}
	switch fieldType {
	case protocol.INT:
		val, _ := strconv.ParseInt(src, 10, 64)
		field.IntVal = &val
	case protocol.DOUBLE:
		val, _ := strconv.ParseFloat(src, 64)
		field.DoubleVal = &val
	case protocol.BOOL:
		val, _ := strconv.ParseBool(src)
		field.BoolVal = &val
	case protocol.STRING:
		field.StrVal = &src
	case protocol.NULL:
		empty := ""
		field.StrVal = &empty
	default:
		panic(fmt.Errorf("Invalid field type"))
	}
	return field
}

func NewValueItem(item *protocol.FieldValue) *ValueItems {
	return &ValueItems{
		Items: []*protocol.FieldValue{item},
	}
}

func ValueItemAppend(valueItems *ValueItems, item *protocol.FieldValue) *ValueItems {
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
