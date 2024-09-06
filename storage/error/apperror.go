package apperror

import "errors"

var (
	ErrEntryNotFound       = errors.New("error entry not found")
	ErrUnknownStorageError = errors.New("error unknown storage error")
)
