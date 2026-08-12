package amqp

// unprocessableError marks a message this service can never act on, however often it comes back. The
// consumer drops those and returns everything else to the queue, so failing to tell them apart either
// discards work that would have succeeded on a retry or spins on a message that never will.
type unprocessableError struct {
	err error
}

func (e *unprocessableError) Error() string {
	return e.err.Error()
}

func (e *unprocessableError) Unwrap() error {
	return e.err
}

// unprocessable marks a failure as one that redelivering the message cannot get past.
func unprocessable(err error) error {
	return &unprocessableError{err: err}
}
