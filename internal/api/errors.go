package api

import (
	"errors"
	"net/http"
)

type StatusError struct {
	StatusCode int
	Err        error
}

func (err *StatusError) Error() string {
	if err == nil {
		return "<nil>"
	}
	if err.Err == nil {
		return http.StatusText(err.StatusCode)
	}
	return err.Err.Error()
}

func (err *StatusError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

func NewStatusError(statusCode int, err error) error {
	if err == nil {
		err = errors.New(http.StatusText(statusCode))
	}
	return &StatusError{
		StatusCode: statusCode,
		Err:        err,
	}
}

func BadRequest(err error) error {
	return NewStatusError(http.StatusBadRequest, err)
}

func NotFound(err error) error {
	return NewStatusError(http.StatusNotFound, err)
}

func ErrorStatus(err error) int {
	var statusErr *StatusError
	if errors.As(err, &statusErr) && statusErr.StatusCode > 0 {
		return statusErr.StatusCode
	}
	return http.StatusServiceUnavailable
}
