package parser

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
)

var (
	doubleRe        = regexp.MustCompile("^[0-9]+\\.[0-9]+")
	intRe           = regexp.MustCompile("^[0-9]+")
	boolRe          = regexp.MustCompile("^(TRUE|FALSE|true|false)")
	stringRe        = regexp.MustCompile("^((\"[^\"]*\")|(\\'[^\\']*\\'))")
	identRe         = regexp.MustCompile("[a-zA-z]+")
	KeywordTokenMap = map[string]int{
		"SELECT": SELECT,
		"UPDATE": UPDATE,
		"DELETE": DELETE,
		"INSERT": INSERT,
		"FROM":   FROM,
		"WHERE":  WHERE,
		"INTO":   INTO,
		"VALUES": VALUES,
	}
	OPTokenMap = map[string]int{
		"(": LP,
		")": RP,
		",": COMMA,
		".": DOT,
	}
	LogicalMap = map[string]int{
		"OR":  OR,
		"AND": AND,
	}
	ComparisonMap = map[string]int{
		"=":  EQUAL,
		">":  GREATER,
		">=": GREATEREQ,
		"<":  SMALLER,
		"<=": SMALLEREQ,
	}
)

type Lex struct {
	Pos       int
	Query     string
	LastToken Token
	LastError string
}

func NewLex(query string) *Lex {
	return &Lex{
		Query: query,
	}
}

func (l *Lex) MkTok(src string) Token {
	t := Token{l.Pos, src}
	l.LastToken = t
	return t
}

func (l *Lex) Lex(lval *FunDBSymType) int {
	if l.Pos >= len(l.Query) {
		return 0
	}
	src := l.Query[l.Pos:]
	cur := strings.TrimLeft(src, " \r\t\n")
	l.Pos += len(src) - len(cur)

	m := doubleRe.FindString(cur)
	if m != "" {
		lval.tok = l.MkTok(m)
		l.Pos += len(m)
		return DOUBLE
	}

	m = intRe.FindString(cur)
	if m != "" {
		lval.tok = l.MkTok(m)
		l.Pos += len(m)
		return INT
	}

	m = boolRe.FindString(cur)
	if m != "" {
		lval.tok = l.MkTok(m)
		l.Pos += len(m)
		return BOOL
	}

	m = stringRe.FindString(cur)
	if m != "" {
		lval.tok = l.MkTok(m)
		l.Pos += len(m)
		return STRING
	}

	for keyword, token := range KeywordTokenMap {
		if strings.HasPrefix(cur, strings.ToUpper(keyword)) {
			lval.tok = l.MkTok(keyword)
			l.Pos += len(keyword)
			return token
		}
	}

	for op, token := range OPTokenMap {
		if strings.HasPrefix(cur, op) {
			lval.tok = l.MkTok(op)
			l.Pos += len(op)
			return token
		}
	}

	for op, token := range ComparisonMap {
		if strings.HasPrefix(cur, op) {
			lval.tok = l.MkTok(op)
			l.Pos += len(op)
			return token
		}
	}

	for op, token := range LogicalMap {
		if strings.HasPrefix(cur, op) {
			lval.tok = l.MkTok(op)
			l.Pos += len(op)
			return token
		}
	}

	m = identRe.FindString(cur)
	if m != "" {
		// fmt.Println("ident")
		lval.tok = l.MkTok(m)
		l.Pos += len(m)
		return IDENT
	}
	return 0
}

func (l *Lex) Error(s string) {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	fmt.Fprintf(buf, "%s at position %v near %s", s, l.Pos, l.LastToken.Src)
	fmt.Printf("Syntax Error: pos near %d:\n", l.Pos)

	fmt.Printf("%s\n", l.Query)
	for i := 0; i < l.Pos; i++ {
		if l.Query[i] == '\t' {
			fmt.Print("\t")
		} else {
			fmt.Print(" ")
		}
	}
	fmt.Println("^")
	l.LastError = buf.String()
}
