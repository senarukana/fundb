package parser

import (
	"fmt"
	"strconv"

	"github.com/senarukana/fundb/protocol"
)

type LiteralNode interface {
	GetVal() *protocol.FieldValue
	GetType() protocol.FieldType
	Compare(int, LiteralNode) bool
	Equal(LiteralNode) bool
	Less(LiteralNode) bool
}

type IntNode struct {
	Type protocol.FieldType
	*protocol.FieldValue
}

type StringNode struct {
	Type protocol.FieldType
	*protocol.FieldValue
}

type BoolNode struct {
	Type protocol.FieldType
	*protocol.FieldValue
}

type DoubleNode struct {
	Type protocol.FieldType
	*protocol.FieldValue
}

type NullNode struct {
	Type protocol.FieldType
	*protocol.FieldValue
}

func (self *IntNode) GetVal() *protocol.FieldValue {
	return self.FieldValue
}

func (self *IntNode) GetType() protocol.FieldType {
	return self.Type
}

func (self *IntNode) Compare(cmpOp int, other LiteralNode) bool {
	switch cmpOp {
	case EQUAL:
		return self.Equal(other)
	case SMALLER:
		return self.Less(other)
	case SMALLEREQ:
		return self.Equal(other) || self.Less(other)
	case GREATER:
		return !(self.Less(other) || self.Equal(other))
	case GREATEREQ:
		return !(self.Less(other))
	default:
		panic("UNKNOWN operator")
	}
}

func (self *IntNode) Equal(other LiteralNode) bool {
	if intNode, ok := other.(*IntNode); ok {
		return self.GetIntVal() == intNode.GetIntVal()
	} else {
		return false
	}
}

func (self *IntNode) Less(other LiteralNode) bool {
	if intNode, ok := other.(*IntNode); ok {
		return self.GetIntVal() < intNode.GetIntVal()
	} else {
		return false
	}
}

func (self *DoubleNode) GetVal() *protocol.FieldValue {
	return self.FieldValue
}

func (self *DoubleNode) GetType() protocol.FieldType {
	return self.Type
}

func (self *DoubleNode) Compare(cmpOp int, other LiteralNode) bool {
	switch cmpOp {
	case EQUAL:
		return self.Equal(other)
	case SMALLER:
		return self.Less(other)
	case SMALLEREQ:
		return self.Equal(other) || self.Less(other)
	case GREATER:
		return !(self.Less(other) || self.Equal(other))
	case GREATEREQ:
		return !(self.Less(other))
	default:
		panic("UNKNOWN operator")
	}
}

func (self *DoubleNode) Equal(other LiteralNode) bool {
	if doubleNode, ok := other.(*DoubleNode); ok {
		return self.GetDoubleVal() == doubleNode.GetDoubleVal()
	} else {
		return false
	}
}

func (self *DoubleNode) Less(other LiteralNode) bool {
	if doubleNode, ok := other.(*DoubleNode); ok {
		return self.GetDoubleVal() < doubleNode.GetDoubleVal()
	} else {
		return false
	}
}

func (self *BoolNode) Equal(other LiteralNode) bool {
	if boolNode, ok := other.(*BoolNode); ok {
		return self.GetBoolVal() == boolNode.GetBoolVal()
	} else {
		return false
	}
}

func (self *BoolNode) Compare(cmpOp int, other LiteralNode) bool {
	switch cmpOp {
	case EQUAL:
		return self.Equal(other)
	case SMALLER:
		return false
	case SMALLEREQ:
		return false
	case GREATER:
		return false
	case GREATEREQ:
		return false
	default:
		panic("UNKNOWN operator")
	}
}

func (self *BoolNode) GetVal() *protocol.FieldValue {
	return self.FieldValue
}

func (self *BoolNode) GetType() protocol.FieldType {
	return self.Type
}

func (self *BoolNode) Less(other LiteralNode) bool {
	return false
}

func (self *StringNode) Equal(other LiteralNode) bool {
	if strNode, ok := other.(*StringNode); ok {
		return self.GetStrVal() == strNode.GetStrVal()
	} else {
		return false
	}
}

func (self *StringNode) Compare(cmpOp int, other LiteralNode) bool {
	switch cmpOp {
	case EQUAL:
		return self.Equal(other)
	case SMALLER:
		return self.Less(other)
	case SMALLEREQ:
		return self.Equal(other) || self.Less(other)
	case GREATER:
		return !(self.Less(other) || self.Equal(other))
	case GREATEREQ:
		return !(self.Less(other))
	default:
		panic("UNKNOWN operator")
	}
}

func (self *StringNode) GetVal() *protocol.FieldValue {
	return self.FieldValue
}

func (self *StringNode) GetType() protocol.FieldType {
	return self.Type
}

func (self *StringNode) Less(other LiteralNode) bool {
	if strNode, ok := other.(*StringNode); ok {
		return self.GetStrVal() < strNode.GetStrVal()
	} else {
		return false
	}
}

func (self *NullNode) GetVal() *protocol.FieldValue {
	return self.FieldValue
}

func (self *NullNode) GetType() protocol.FieldType {
	return self.Type
}

func (self *NullNode) Equal(other LiteralNode) bool {
	if _, ok := other.(*NullNode); ok {
		return true
	} else {
		return false
	}
}

func (self *NullNode) Compare(cmpOp int, other LiteralNode) bool {
	switch cmpOp {
	case EQUAL:
		return self.Equal(other)
	case SMALLER:
		return false
	case SMALLEREQ:
		return false
	case GREATER:
		return false
	case GREATEREQ:
		return false
	default:
		panic("UNKNOWN operator")
	}
}

func (self *NullNode) Less(other LiteralNode) bool {
	return false
}

func NewLiteral(fieldType protocol.FieldType, src string) LiteralNode {
	field := &protocol.FieldValue{}
	switch fieldType {
	case protocol.INT:
		val, _ := strconv.ParseInt(src, 10, 64)
		field.IntVal = &val
		return &IntNode{protocol.INT, field}
	case protocol.DOUBLE:
		val, _ := strconv.ParseFloat(src, 64)
		field.DoubleVal = &val
		return &DoubleNode{protocol.DOUBLE, field}
	case protocol.BOOL:
		val, _ := strconv.ParseBool(src)
		field.BoolVal = &val
		return &BoolNode{protocol.BOOL, field}
	case protocol.STRING:
		field.StrVal = &src
		return &StringNode{protocol.STRING, field}
	case protocol.NULL:
		return &NullNode{protocol.NULL, field}
	default:
		panic(fmt.Errorf("Invalid field type"))
	}
	panic("shouldn't go here")
}
