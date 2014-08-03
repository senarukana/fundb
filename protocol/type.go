package protocol

type FieldType int

const (
	NULL FieldType = iota
	INT
	DOUBLE
	STRING
	BOOL
)
