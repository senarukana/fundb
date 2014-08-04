package engine

import (
	"errors"
)

var (
	ErrUnknownDBEngineName = errors.New("unknown db engine")
	ErrDbInitError         = errors.New("init db engine error")
	ErrKeyNotFound         = errors.New("key not found")
	CursorEnd              = errors.New("cursor already seeks to end")
	ErrCursorAlreadOpened  = errors.New("cursor already opened")
)
