package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/cyverse-de/p/go/ptypes"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/p/go/svcerror"
	"github.com/cyverse-de/resource-usage-api/config"
	"github.com/pkg/errors"
	"github.com/samber/lo"
)

// dataSizeResource is the QMS resource type used for data store usage.
const dataSizeResource = "data.size"

// requestTimeout matches the timeout the NATS request/reply calls this client replaced used. Several
// of these calls sit on request paths, so an unresponsive subscriptions must not pin them.
const requestTimeout = 30 * time.Second

// maxErrorBodySize caps how much of an error response body is read while looking for the error envelope.
const maxErrorBodySize = 64 * 1024

// Subscriptions is a client for the subscriptions service.
type Subscriptions struct {
	baseURL *url.URL
	client  *http.Client
	config  *config.Config
}

// SubscriptionsClient returns a new instance of Subscriptions for the given raw base URL.
func SubscriptionsClient(baseURL string, cfg *config.Config) (*Subscriptions, error) {
	parsed, err := parseBaseURL(baseURL)
	if err != nil {
		return nil, err
	}
	return &Subscriptions{
		baseURL: parsed,
		client:  &http.Client{Transport: http.DefaultTransport, Timeout: requestTimeout},
		config:  cfg,
	}, nil
}

// serviceError converts a populated response error envelope into an error. subscriptions normally maps the
// envelope to a non-2xx status; this defends against a failure envelope arriving with a 2xx status anyway.
func serviceError(serr *svcerror.ServiceError) error {
	if serr == nil || serr.ErrorCode == svcerror.ErrorCode_UNSET {
		return nil
	}
	if serr.StatusCode != 0 {
		return NewHTTPError(int(serr.StatusCode), serr.Message)
	}
	return errors.New(serr.Message)
}

// do sends the request and decodes the response envelope into out.
func (c *Subscriptions) do(ctx context.Context, method string, reqURL *url.URL, body io.Reader, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, reqURL.String(), body)
	if err != nil {
		return errors.Wrapf(err, "unable to build the request for %s", reqURL)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return errors.Wrapf(err, "unable to send the request to %s", reqURL)
	}
	defer resp.Body.Close() // nolint: errcheck

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// Error responses carry the same envelope in the body; surface its message so failures can be
		// triaged from logs without querying subscriptions.
		message := fmt.Sprintf("%s returned %d", reqURL, resp.StatusCode)
		var envelope struct {
			Error *svcerror.ServiceError `json:"error"`
		}
		decoder := json.NewDecoder(io.LimitReader(resp.Body, maxErrorBodySize))
		if decodeErr := decoder.Decode(&envelope); decodeErr == nil && envelope.Error.GetMessage() != "" {
			message = fmt.Sprintf("%s: %s", message, envelope.Error.GetMessage())
		}
		return NewHTTPError(resp.StatusCode, message)
	}

	if err = json.NewDecoder(resp.Body).Decode(out); err != nil {
		return errors.Wrapf(err, "unable to parse the response body from %s", reqURL)
	}

	return nil
}

// AddUserUpdate records a usage or quota update for a user. The username in the request path is
// authoritative; subscriptions overwrites whatever username the update body carries.
func (c *Subscriptions) AddUserUpdate(ctx context.Context, username string, update *qms.Update) error {
	user := c.config.FixUsername(username)

	body, err := json.Marshal(&qms.AddUpdateRequest{Update: update})
	if err != nil {
		return errors.Wrap(err, "unable to marshal the user update")
	}

	var response qms.AddUpdateResponse
	if err = c.do(ctx, http.MethodPut, c.baseURL.JoinPath("user", user, "updates"), bytes.NewReader(body), &response); err != nil {
		return err
	}

	return serviceError(response.Error)
}

// UserCurrentDataUsage returns the user's current data.size usage as recorded in QMS. It returns a
// NoUsageRecordedError when QMS has no data.size usage for the user; callers use that to trigger an
// asynchronous refresh.
func (c *Subscriptions) UserCurrentDataUsage(ctx context.Context, username string) (*UserDataUsage, error) {
	user := c.config.FixUsername(username)

	var response qms.UsageList
	if err := c.do(ctx, http.MethodGet, c.baseURL.JoinPath("users", user, "usages"), nil, &response); err != nil {
		return nil, err
	}
	if err := serviceError(response.Error); err != nil {
		return nil, err
	}

	var usage *qms.Usage
	for _, u := range response.Usages {
		if u.ResourceType != nil && u.ResourceType.Name == dataSizeResource {
			usage = u
		}
	}

	if usage == nil {
		return nil, &NoUsageRecordedError{Username: user, ResourceType: dataSizeResource}
	}

	createdAt := usage.CreatedAt.AsTime()
	lastModified := usage.LastModifiedAt.AsTime()

	return &UserDataUsage{
		ID:           usage.Uuid,
		Total:        int64(usage.Usage),
		Time:         &createdAt,
		LastModified: &lastModified,
	}, nil
}

// AllResourceOveragesForUser returns every resource the user is currently over quota on.
func (c *Subscriptions) AllResourceOveragesForUser(ctx context.Context, username string) (*qms.OverageList, error) {
	user := c.config.FixUsername(username)

	var response qms.OverageList
	if err := c.do(ctx, http.MethodGet, c.baseURL.JoinPath("users", user, "overages"), nil, &response); err != nil {
		return nil, err
	}
	if err := serviceError(response.Error); err != nil {
		return nil, err
	}

	return &response, nil
}

// UpdateUsageForUser records the user's data.size usage in QMS, replacing any previous value.
func (c *Subscriptions) UpdateUsageForUser(ctx context.Context, username string, usageValue float64) (*UserDataUsage, error) {
	user := c.config.FixUsername(username)

	request := &qms.AddUpdateRequest{
		Update: &qms.Update{
			ValueType:     "usages",
			Value:         usageValue,
			EffectiveDate: ptypes.Now(),
			Operation:     &qms.UpdateOperation{Name: "SET"},
			ResourceType:  &qms.ResourceType{Name: dataSizeResource, Unit: "bytes"},
			User:          &qms.QMSUser{Username: user},
		},
	}

	body, err := json.Marshal(request)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal the usage update")
	}

	var response qms.AddUpdateResponse
	if err = c.do(ctx, http.MethodPut, c.baseURL.JoinPath("user", user, "updates"), bytes.NewReader(body), &response); err != nil {
		return nil, err
	}
	if err = serviceError(response.Error); err != nil {
		return nil, err
	}
	if response.Update == nil {
		return nil, errors.New("subscriptions returned no update for the recorded usage")
	}

	effectiveDate := response.Update.EffectiveDate.AsTime()

	return &UserDataUsage{
		ID:           response.Update.Uuid,
		Total:        int64(response.Update.Value),
		Time:         &effectiveDate,
		LastModified: &effectiveDate,
	}, nil
}

// AddUserUpdatesBatch records usage for each user in usages, continuing past individual failures.
func (c *Subscriptions) AddUserUpdatesBatch(ctx context.Context, usages map[string]float64) ([]*UserDataUsage, error) {
	keys := lo.Keys(usages)
	retval := make([]*UserDataUsage, 0, len(keys))
	errs := make([]error, 0)
	for _, k := range keys {
		u, err := c.UpdateUsageForUser(ctx, k, usages[k])
		if err != nil {
			errs = append(errs, err)
			continue
		}
		retval = append(retval, u)
	}
	// If we got errors, throw the first one. It's a little ugly but is the
	// error that'd get thrown if we were doing it in the loop anyway.
	if len(errs) > 0 {
		return retval, errs[0]
	}
	return retval, nil
}

// GetSubscriptionSummary returns the user's full subscription, including quotas and usages.
func (c *Subscriptions) GetSubscriptionSummary(ctx context.Context, username string) (*qms.SubscriptionResponse, error) {
	user := c.config.FixUsername(username)

	var response qms.SubscriptionResponse
	if err := c.do(ctx, http.MethodGet, c.baseURL.JoinPath("summary", user), nil, &response); err != nil {
		return nil, err
	}
	if err := serviceError(response.Error); err != nil {
		return nil, err
	}

	return &response, nil
}
