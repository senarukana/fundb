package parser

import (
	"github.com/senarukana/fundb/protocol"
)

type Literal struct {
	Pos   int
	Type  protocol.FieldType
	Value string
}

type Ident struct {
	Pos  int
	Name string
}

type ValueItems struct {
	items []*Literal
}

type ValueList struct {
	values []*ValueItems
}

type ColumnFields struct {
	fields []*Ident
}

type InsertQuery struct {
	Table     *Ident
	Fields    *ColumnFields
	ValueList *ValueList
}

func NewColumnField(field *Ident) *ColumnFields {
	return &ColumnFields{
		fields: []*Ident{field},
	}
}

func ColumnFieldsAppend(columnFields *ColumnFields, field *Ident) *ColumnFields {
	if columnFields == nil {
		return NewColumnField(field)
	}
	columnFields.fields = append(columnFields.fields, field)
	return columnFields
}

func NewValueItem(item *Literal) *ValueItems {
	return &ValueItems{
		items: []*Literal{item},
	}
}

func ValueItemAppend(valueItems *ValueItems, item *Literal) *ValueItems {
	if valueItems == nil {
		return NewValueItem(item)
	}
	valueItems.items = append(valueItems.items, item)
	return valueItems
}

func NewValueList(items *ValueItems) *ValueList {
	return &ValueList{
		values: []*ValueItems{items},
	}
}

func ValueListAppend(valueList *ValueList, items *ValueItems) *ValueList {
	if valueList == nil {
		return NewValueList(items)
	}
	valueList.values = append(valueList.values, items)
	return valueList
}
