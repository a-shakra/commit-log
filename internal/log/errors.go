package log

import "errors"

var (
	ErrEndOfFile = errors.New("no record stored at this position")
	ErrFileFull  = errors.New("cannot process this write operation without exceeding maximum size")
)
