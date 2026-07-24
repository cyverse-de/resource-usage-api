package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/p/go/svcerror"
	"github.com/pkg/errors"
)

// Subscriptions is a client for the subscriptions service.
type Subscriptions struct {
	baseURL *url.URL
}

// SubscriptionsClient returns a new instance of Subscriptions for the given raw base URL.
func SubscriptionsClient(baseURL string) (*Subscriptions, error) {

	// Parse the raw base URL.
	url, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	// Ensure that the base URL path doesn't end with a slash.
	url.Path = strings.TrimSuffix(url.Path, "/")

	return &Subscriptions{baseURL: url}, nil
}

// subscriptionsURL returns a URL that can be used to connect to the subscriptions service. The URL path is
// determined by the base URL and the arguments.
func (c *Subscriptions) subscriptionsURL(components ...string) *url.URL {
	return BuildURL(c.baseURL, components...)
}

// serviceError converts a populated response error envelope into an error. subscriptions reports request failures
// in the response body as well as the status code, so a 2xx response can still describe a failure.
func serviceError(serr *svcerror.ServiceError) error {
	if serr == nil || serr.ErrorCode == svcerror.ErrorCode_UNSET {
		return nil
	}
	if serr.StatusCode != 0 {
		return NewHTTPError(int(serr.StatusCode), serr.Message)
	}
	return errors.New(serr.Message)
}

// AddUserUpdate records a usage or quota update for a user. The username in the request path is authoritative;
// subscriptions overwrites whatever username the update body carries.
func (c *Subscriptions) AddUserUpdate(ctx context.Context, username string, update *qms.Update) error {
	requestURL := c.subscriptionsURL("user", StripUsernameSuffix(username), "updates")

	body, err := json.Marshal(&qms.AddUpdateRequest{Update: update})
	if err != nil {
		return errors.Wrap(err, "unable to marshal the user update")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, requestURL.String(), bytes.NewReader(body))
	if err != nil {
		return errors.Wrapf(err, "unable to build the request for %s", requestURL)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return errors.Wrapf(err, "unable to send the request to %s", requestURL)
	}
	defer resp.Body.Close() // nolint: errcheck

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return NewHTTPError(resp.StatusCode, fmt.Sprintf("%s returned %d", requestURL, resp.StatusCode))
	}

	var response qms.AddUpdateResponse
	if err = json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return errors.Wrapf(err, "unable to parse the response body from %s", requestURL)
	}

	return serviceError(response.Error)
}
