package gtfsrt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"
)

// maxBodyBytes limits one feed response. The largest feed that this exporter
// was tested against is the SNCF alert feed at 900 KB, so 64 MB is generous.
const maxBodyBytes = 64 << 20

// ErrForbiddenRedirect reports that a target redirected to a host that the
// configuration does not allow.
//
// The check matters because the exporter fetches a URL that the caller gives.
// Without it, a hostile target answers with a redirect to an address inside
// the network of the exporter, and the exporter follows.
var ErrForbiddenRedirect = errors.New("gtfsrt: the redirect target is not in allowed_hosts")

// Client reads GTFS-Realtime feeds over HTTP.
type Client struct {
	// reuse keeps connections alive between scrapes. fresh closes each one.
	reuse *http.Client
	fresh *http.Client
	// requestTimeout is the budget for one feed when the caller names none.
	requestTimeout time.Duration
	userAgent      string
}

// NewClient returns a client that gives up on one feed after the timeout.
//
// allowHost decides whether the client may follow a redirect to a host. A nil
// function allows every host. The check runs on every hop, because only the
// first URL passed through the handler.
//
// The transport reads the proxy settings from the environment, so the
// variables HTTP_PROXY, HTTPS_PROXY, and NO_PROXY work.
func NewClient(timeout time.Duration, userAgent string, allowHost func(string) bool) *Client {
	transport := func(keepAlive bool) *http.Transport {
		return &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			ForceAttemptHTTP2:     true,
			DisableKeepAlives:     !keepAlive,
			MaxIdleConnsPerHost:   4,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
	}
	check := func(request *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("gtfsrt: too many redirects")
		}
		if allowHost != nil && !allowHost(request.URL.Hostname()) {
			return fmt.Errorf("%w: %s", ErrForbiddenRedirect, request.URL.Hostname())
		}
		return nil
	}
	return &Client{
		reuse:          &http.Client{Transport: transport(true), CheckRedirect: check},
		fresh:          &http.Client{Transport: transport(false), CheckRedirect: check},
		requestTimeout: timeout,
		userAgent:      userAgent,
	}
}

// FetchOptions holds the read settings of one feed.
type FetchOptions struct {
	// Headers are added to the request. Use them for an API key or for a
	// client name that the operator asks for.
	Headers map[string]string
	// RequestTimeout is the budget for this feed. A value of 0 or less uses
	// the default of the client.
	RequestTimeout time.Duration
	// NoKeepAlive opens a new connection for the feed.
	NoKeepAlive bool
}

// Fetch reads one feed and decodes it.
//
// The snapshot is never nil. On failure it carries the size and the duration
// of the attempt, so that the collector reports how long a broken feed took.
func (c *Client) Fetch(ctx context.Context, feedURL string, options FetchOptions) (*Snapshot, error) {
	started := time.Now()
	snapshot := &Snapshot{}
	finish := func() {
		snapshot.Duration = time.Since(started)
		snapshot.FetchedAt = time.Now()
	}

	budget := options.RequestTimeout
	if budget <= 0 {
		budget = c.requestTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, feedURL, nil)
	if err != nil {
		finish()
		return snapshot, fmt.Errorf("gtfsrt: cannot build a request for %s: %w", feedURL, err)
	}
	request.Header.Set("User-Agent", c.userAgent)
	for name, value := range options.Headers {
		request.Header.Set(name, value)
	}

	client := c.reuse
	if options.NoKeepAlive {
		client = c.fresh
	}
	response, err := client.Do(request)
	if err != nil {
		finish()
		// A redirect refusal arrives wrapped in a *url.Error. Unwrap it, so
		// that the caller can test it with errors.Is.
		var urlErr *url.Error
		if errors.As(err, &urlErr) && errors.Is(urlErr.Err, ErrForbiddenRedirect) {
			return snapshot, fmt.Errorf("gtfsrt: %s: %w", feedURL, urlErr.Err)
		}
		return snapshot, fmt.Errorf("gtfsrt: cannot fetch %s: %w", feedURL, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxBodyBytes))
		_ = response.Body.Close()
	}()

	if response.StatusCode != http.StatusOK {
		finish()
		return snapshot, fmt.Errorf("gtfsrt: %s returned HTTP %d", feedURL, response.StatusCode)
	}

	// Do not test the content type. Producers send application/x-protobuf,
	// application/octet-stream, and others for the same payload.
	body, err := io.ReadAll(io.LimitReader(response.Body, maxBodyBytes))
	snapshot.Bytes = len(body)
	if err != nil {
		finish()
		return snapshot, fmt.Errorf("gtfsrt: cannot read %s: %w", feedURL, err)
	}
	if len(body) == maxBodyBytes {
		finish()
		return snapshot, fmt.Errorf("gtfsrt: %s is larger than the limit of %d bytes", feedURL, maxBodyBytes)
	}

	message := &gtfs.FeedMessage{}
	if err := proto.Unmarshal(body, message); err != nil {
		finish()
		return snapshot, fmt.Errorf("gtfsrt: cannot decode %s: %w", feedURL, err)
	}
	snapshot.Message = message
	finish()
	return snapshot, nil
}
