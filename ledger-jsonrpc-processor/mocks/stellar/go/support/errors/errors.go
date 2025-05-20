package errors

// Wrap wraps an error with a message
func Wrap(err error, message string) error {
	return err
}

// Wrapf wraps an error with a formatted message
func Wrapf(err error, format string, args ...interface{}) error {
	return err
}