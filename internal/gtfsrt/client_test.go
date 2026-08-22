package gtfsrt_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"

	"github.com/raspbeguy/gtfs_exporter/internal/gtfsrt"
)

func feedBody(t *testing.T) []byte {
	t.Helper()
	body, err := proto.Marshal(&gtfs.FeedMessage{Header: &gtfs.FeedHeader{
		GtfsRealtimeVersion: proto.String("2.0"),
		Timestamp:           proto.Uint64(1787412524),
	}})
	if err != nil {
		t.Fatalf("cannot marshal: %v", err)
	}
	return body
}

func TestFetchDecodesAFeed(t *testing.T) {
	body := feedBody(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Producers send several content types for the same payload, so the
		// client must not test this header.
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	client := gtfsrt.NewClient(5*time.Second, "test", nil)
	snapshot, err := client.Fetch(context.Background(), server.URL, gtfsrt.FetchOptions{})
	if err != nil {
		t.Fatalf("got error %v, want none", err)
	}
	if !snapshot.OK() {
		t.Fatal("the snapshot must hold a message")
	}
	if got := snapshot.Header().GetGtfsRealtimeVersion(); got != "2.0" {
		t.Errorf("got version %q, want 2.0", got)
	}
	if snapshot.Bytes != len(body) {
		t.Errorf("got %d bytes, want %d", snapshot.Bytes, len(body))
	}
}

// TestAFailedFetchStillReportsTheAttempt checks that the caller can always
// report how long a broken feed took.
func TestAFailedFetchStillReportsTheAttempt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "gone", http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)

	client := gtfsrt.NewClient(5*time.Second, "test", nil)
	snapshot, err := client.Fetch(context.Background(), server.URL, gtfsrt.FetchOptions{})
	if err == nil {
		t.Fatal("got no error, want one")
	}
	if snapshot == nil {
		t.Fatal("the snapshot must never be nil")
	}
	if snapshot.OK() {
		t.Error("a failed read must hold no message")
	}
	if snapshot.Duration <= 0 {
		t.Error("the duration of the attempt must be reported")
	}
}

// TestARedirectToAForbiddenHostIsRejected guards the exporter against a target
// that sends it somewhere else.
//
// The endpoint fetches a URL that the caller gives. Without a test on each
// hop, a hostile target answers with a redirect to an address inside the
// network of the exporter, and the exporter follows it.
func TestARedirectToAForbiddenHostIsRejected(t *testing.T) {
	secret := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(feedBody(t))
	}))
	t.Cleanup(secret.Close)

	// Both test servers listen on the loopback address, so name the second one
	// through localhost. The two hostnames then differ, the way they would for
	// two real operators.
	elsewhere := strings.Replace(secret.URL, "127.0.0.1", "localhost", 1)

	var reached bool
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		http.Redirect(w, r, elsewhere, http.StatusFound)
	}))
	t.Cleanup(entry.Close)

	// The entry host is allowed. The redirect target is not.
	entryHost := mustHost(t, entry.URL)
	client := gtfsrt.NewClient(5*time.Second, "test", func(host string) bool {
		return host == entryHost
	})
	_, err := client.Fetch(context.Background(), entry.URL, gtfsrt.FetchOptions{})
	if !errors.Is(err, gtfsrt.ErrForbiddenRedirect) {
		t.Fatalf("got error %v, want a forbidden redirect", err)
	}
	if !reached {
		t.Error("the first hop must still be made")
	}
}

// TestARedirectToAnAllowedHostIsFollowed checks that the guard does not break
// a normal redirect. The static feed of data.gouv.fr redirects to another
// host, so redirects are ordinary.
func TestARedirectToAnAllowedHostIsFollowed(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(feedBody(t))
	}))
	t.Cleanup(target.Close)
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	t.Cleanup(entry.Close)

	client := gtfsrt.NewClient(5*time.Second, "test", func(string) bool { return true })
	snapshot, err := client.Fetch(context.Background(), entry.URL, gtfsrt.FetchOptions{})
	if err != nil {
		t.Fatalf("got error %v, want none", err)
	}
	if !snapshot.OK() {
		t.Error("the snapshot must hold a message")
	}
}

// TestTheRequestCarriesTheConfiguredHeaders checks the module settings that a
// query string must not carry, such as an API key.
func TestTheRequestCarriesTheConfiguredHeaders(t *testing.T) {
	var got http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		_, _ = w.Write(feedBody(t))
	}))
	t.Cleanup(server.Close)

	client := gtfsrt.NewClient(5*time.Second, "gtfs_exporter/test", nil)
	_, err := client.Fetch(context.Background(), server.URL, gtfsrt.FetchOptions{
		Headers: map[string]string{"X-Api-Key": "secret"},
	})
	if err != nil {
		t.Fatalf("got error %v, want none", err)
	}
	if got.Get("X-Api-Key") != "secret" {
		t.Errorf("got api key %q, want secret", got.Get("X-Api-Key"))
	}
	if got.Get("User-Agent") != "gtfs_exporter/test" {
		t.Errorf("got user agent %q, want gtfs_exporter/test", got.Get("User-Agent"))
	}
}

// TestAHungFeedIsAlwaysBounded checks that one slow operator cannot hold a
// scrape open. One of twelve polls of the Nantes feed returned nothing inside
// a single minute, so a stall is ordinary.
func TestAHungFeedIsAlwaysBounded(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(server.Close)

	client := gtfsrt.NewClient(50*time.Millisecond, "test", nil)
	done := make(chan error, 1)
	go func() {
		_, err := client.Fetch(context.Background(), server.URL, gtfsrt.FetchOptions{})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Error("got no error, want a timeout")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the fetch did not stop at its budget")
	}
}

func mustHost(t *testing.T, raw string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("cannot parse %s: %v", raw, err)
	}
	return parsed.Hostname()
}
