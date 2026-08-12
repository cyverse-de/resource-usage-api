package internal

import (
	"context"
	"net/http"

	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/pkg/errors"
)

// errorResponse builds a response carrying both the status to send and the matching error code.
func errorResponse(status int, message string) logging.ErrorResponse {
	return logging.ErrorResponse{
		Message:        message,
		ErrorCode:      status,
		HTTPStatusCode: status,
	}
}

// internalError reports a failure the caller can do nothing about. The underlying error is logged and
// kept out of the response, which would otherwise hand out schema and constraint names from the
// databases and error text from the services behind this one.
func internalError(ctx context.Context, err error, message string) logging.ErrorResponse {
	log.WithContext(ctx).Error(errors.Wrap(err, message))
	return errorResponse(http.StatusInternalServerError, message)
}
