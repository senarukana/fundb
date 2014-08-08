package leveldb

import (
	"fmt"

	"github.com/senarukana/fundb/parser"
	"github.com/senarukana/fundb/protocol"
	"github.com/senarukana/fundb/util"

	// "github.com/golang/glog"
)

func NewLiteral(field *protocol.FieldValue) parser.LiteralNode {
	var literal parser.LiteralNode
	if field.IntVal != nil {
		literal = &parser.IntNode{
			protocol.INT,
			field,
		}
	} else if field.DoubleVal != nil {
		literal = &parser.DoubleNode{
			protocol.DOUBLE,
			field,
		}
	} else if field.BoolVal != nil {
		literal = &parser.BoolNode{
			protocol.BOOL,
			field,
		}
	} else if field.StrVal != nil {
		literal = &parser.StringNode{
			protocol.STRING,
			field,
		}
	} else {
		literal = &parser.NullNode{
			protocol.NULL,
			field,
		}
	}
	return literal
}

func getFieldValue(record *protocol.Record, fieldName string, fields []string) (parser.LiteralNode, error) {
	fieldIdx := -1
	for i, field := range fields {
		if field == fieldName {
			fieldIdx = i
			break
		}
	}
	if fieldIdx == -1 {
		panic(fmt.Errorf("field %s not found", fieldName))
	}

	return NewLiteral(record.Values[fieldIdx]), nil
}

func getScalarValue(record *protocol.Record, scalar *parser.Scalar) (parser.LiteralNode, error) {
	if scalar.Type != parser.SCLAR_LITERAL {
		panic("NOT SUPPORTED SCALAR TYPE")
	}
	return scalar.Val.(parser.LiteralNode), nil
}

func getExpressionValue(record *protocol.Record, expression interface{}, fields []string) (parser.LiteralNode, error) {
	if fieldName, ok := expression.(string); ok {
		return getFieldValue(record, fieldName, fields)
	} else if scalar, ok := expression.(*parser.Scalar); ok {
		return getScalarValue(record, scalar)
	} else {
		panic(fmt.Sprintf("unsupported expression value %v", expression))
	}
}

func matchComparison(record *protocol.Record, condition *parser.WhereExpression, fields []string) (bool, error) {
	leftVal, err := getExpressionValue(record, condition.Left, fields)
	if err != nil {
		return false, err
	}
	rightVal, err := getExpressionValue(record, condition.Right, fields)
	if err != nil {
		return false, err
	}
	// glog.Errorf("left:%v, right:%v\n", leftVal, rightVal)
	return leftVal.Compare(parser.ComparisonMap[condition.Token.Src], rightVal), nil
}

func betweenComparison(record *protocol.Record, condition *parser.WhereExpression, fields []string) (bool, error) {
	recordVal, err := getExpressionValue(record, condition.Left, fields)
	if err != nil {
		return false, err
	}

	betweenExpr := condition.Right.(*parser.BetweenExpression)
	if betweenExpr.Left.Type != parser.SCLAR_LITERAL || betweenExpr.Right.Type != parser.SCLAR_LITERAL {
		panic("NOT SUPPORTED SCALAR TYPE")
	}
	leftVal := betweenExpr.Left.Val.(parser.LiteralNode)
	rightVal := betweenExpr.Right.Val.(parser.LiteralNode)

	if recordVal.Compare(parser.GREATEREQ, leftVal) && recordVal.Compare(parser.SMALLER, rightVal) {
		return true, nil
	} else {
		return false, nil
	}
}

func match(record *protocol.Record, condition *parser.WhereExpression, fields []string) (bool, error) {
	switch condition.Type {
	case parser.WHERE_BETWEEN:
		return betweenComparison(record, condition, fields)
	case parser.WHERE_COMPARISON:
		return matchComparison(record, condition, fields)
	case parser.WHERE_AND:
		if matched, err := match(record, condition.Left.(*parser.WhereExpression), fields); !matched || err != nil {
			return matched, err
		} else {
			return match(record, condition.Right.(*parser.WhereExpression), fields)
		}
	default:
		panic(fmt.Errorf("UNKNOWN Where Type"))
	}
	panic("shouldn't go here")
}

func filterFields(records []*protocol.Record, selectFields, fetchFields []string) []*protocol.Record {
	selectFieldSet := util.NewStringSetFromStrings(selectFields)
	for _, record := range records {
		newValues := make([]*protocol.FieldValue, 0, len(selectFields))
		for i, field := range fetchFields {
			if !selectFieldSet.Exists(field) {
				continue
			}
			newValues = append(newValues, record.Values[i])
		}
		record.Values = newValues
	}
	return records
}

func filterCondition(records []*protocol.Record, condition *parser.WhereExpression, fields []string) ([]*protocol.Record, error) {
	if condition == nil {
		return records, nil
	} else {
		var res []*protocol.Record
		for _, record := range records {
			matched, err := match(record, condition, fields)
			if err != nil {
				return nil, err
			}
			if matched {
				res = append(res, record)
			}
		}
		return res, nil
	}
}
