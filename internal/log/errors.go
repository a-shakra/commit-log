package log

import (
	"errors"
	"fmt"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

var (
	ErrEndOfFile = errors.New("no record stored at this position")
	ErrFileFull  = errors.New("cannot process this write operation without exceeding maximum size")
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	st := status.New(
		404,
		fmt.Sprintf("offset out of range %d", e.Offset),
	)
	msg := fmt.Sprintf(
		"inputted offset is outside log range: %d",
		e.Offset,
	)
	details := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}
	stWithDetails, err := st.WithDetails(details)
	if err != nil {
		return st
	}
	return stWithDetails
}

func (e ErrOffsetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
