package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/pkg/errors"
)

// QMSAPI represents an instance of a QMS API client.
type QMSAPI struct {
	baseURL *url.URL
}

// QMSAPIClient returns a new QMSAPI instance.
func QMSAPIClient(baseURL string) (*QMSAPI, error) {

	//  Parse the raw base URL.
	url, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	// Ensure that the base URL path doesn't end with a slash.
	url.Path = strings.TrimSuffix(url.Path, "/")

	return &QMSAPI{baseURL: url}, nil
}

type SubscriptionResult struct {
	Result Subscription `json:"result"`
}

// qmsURL returns a URL that can be used to connect to QMS. The URL path is determined by the base URL and the path
// components in the argument list.
func (c QMSAPI) qmsURL(components ...string) *url.URL {
	return BuildURL(c.baseURL, components...)
}

// GetSubscription retrieves the subscription information for the given user.
func (c *QMSAPI) GetSubscription(ctx context.Context, username string) (*Subscription, error) {
	var upr SubscriptionResult

	// Build the request.
	requestURL := c.qmsURL("v1", "users", StripUsernameSuffix(username), "plan")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return &upr.Result, errors.Wrapf(err, "unable to build the request for %s", requestURL)
	}

	// Get the response.
	resp, err := client.Do(req)
	if err != nil {
		return &upr.Result, errors.Wrapf(err, "unable to send the request to %s", requestURL)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &upr.Result, NewHTTPError(resp.StatusCode, fmt.Sprintf("%s returned %d", requestURL, resp.StatusCode))
	}

	// Read the response body.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return &upr.Result, errors.Wrapf(err, "unable to read the response from %s", requestURL)
	}

	// Unmarshal the response body.
	err = json.Unmarshal(body, &upr)
	if err != nil {
		return &upr.Result, errors.Wrapf(err, "unable to parse the response from %s", requestURL)
	}

	return &upr.Result, nil
}
