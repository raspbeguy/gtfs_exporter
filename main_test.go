package main

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"

	"github.com/raspbeguy/gtfs_exporter/internal/gtfsrt"
	"github.com/raspbeguy/gtfs_exporter/internal/gtfsstatic"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("cannot write the configuration: %v", err)
	}
	return path
}

func TestLoadConfigFillsTheDefaults(t *testing.T) {
	config, err := loadConfig(writeConfig(t, ""))
	if err != nil {
		t.Fatalf("got error %v, want none", err)
	}
	if config.ListenAddress != ":10053" {
		t.Errorf("got address %q, want :10053", config.ListenAddress)
	}
	if config.Timeout != 30*time.Second {
		t.Errorf("got timeout %v, want 30s", config.Timeout)
	}
	if config.MaxInFlight != 4 {
		t.Errorf("got max_in_flight %d, want 4", config.MaxInFlight)
	}
}

// TestLoadConfigRejectsAnUnknownKey checks that a typo stops the exporter at
// start, rather than silently dropping a setting.
func TestLoadConfigRejectsAnUnknownKey(t *testing.T) {
	_, err := loadConfig(writeConfig(t, "listen_adress: \":1\"\n"))
	if err == nil {
		t.Fatal("got no error, want one for the unknown key")
	}
}

func TestLoadConfigRejectsABadSetting(t *testing.T) {
	testCases := []struct {
		name string
		body string
	}{
		{"request timeout above the scrape budget", "timeout: 5s\nrequest_timeout: 10s\n"},
		{"delay buckets that do not increase", "modules:\n  m:\n    delay_buckets: [0, 60, 30]\n"},
		{"delay buckets that repeat", "modules:\n  m:\n    delay_buckets: [0, 60, 60]\n"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := loadConfig(writeConfig(t, testCase.body)); err == nil {
				t.Error("got no error, want one")
			}
		})
	}
}

func TestSelectModule(t *testing.T) {
	modules := map[string]Module{"default": {PerRoute: false}, "big": {PerRoute: true}}
	if module, err := selectModule(modules, ""); err != nil || module.PerRoute {
		t.Errorf("an empty name must select default, got %+v and %v", module, err)
	}
	if module, err := selectModule(modules, "big"); err != nil || !module.PerRoute {
		t.Errorf("got %+v and %v, want the big module", module, err)
	}
	if _, err := selectModule(modules, "absent"); err == nil {
		t.Error("got no error for an unknown module, want one")
	}
}

// TestTheScrapeTimeoutHeaderLowersTheBudget checks that the exporter respects
// the deadline that Prometheus sends.
//
// A budget above that value spends work on a request that Prometheus has
// already abandoned.
func TestTheScrapeTimeoutHeaderLowersTheBudget(t *testing.T) {
	testCases := []struct {
		name       string
		header     string
		configured time.Duration
		want       time.Duration
	}{
		{"no header keeps the configured budget", "", 30 * time.Second, 30 * time.Second},
		{"a lower header wins", "5", 30 * time.Second, 5 * time.Second},
		{"a higher header does not raise the budget", "60", 30 * time.Second, 30 * time.Second},
		{"a fractional header is read", "2.5", 30 * time.Second, 2500 * time.Millisecond},
		{"a broken header keeps the configured budget", "soon", 30 * time.Second, 30 * time.Second},
		{"a zero header keeps the configured budget", "0", 30 * time.Second, 30 * time.Second},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/probe", nil)
			if testCase.header != "" {
				request.Header.Set("X-Prometheus-Scrape-Timeout-Seconds", testCase.header)
			}
			if got := scrapeBudget(request, testCase.configured); got != testCase.want {
				t.Errorf("got %v, want %v", got, testCase.want)
			}
		})
	}
}

func testFeedServer(t *testing.T) *httptest.Server {
	t.Helper()
	body, err := proto.Marshal(&gtfs.FeedMessage{Header: &gtfs.FeedHeader{
		GtfsRealtimeVersion: proto.String("2.0"),
		Timestamp:           proto.Uint64(1787412524),
	}})
	if err != nil {
		t.Fatalf("cannot marshal: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	return server
}

func testHandler(t *testing.T, config *Config) http.HandlerFunc {
	t.Helper()
	allowed := map[string]bool{}
	for _, host := range config.AllowedHosts {
		allowed[strings.ToLower(host)] = true
	}
	allowHost := func(host string) bool {
		return len(allowed) == 0 || allowed[strings.ToLower(host)]
	}
	client := gtfsrt.NewClient(config.RequestTimeout, "test", allowHost)
	static := gtfsstatic.NewRegistry(t.Context(), http.DefaultClient, slog.New(slog.DiscardHandler))
	return probeHandler(client, static, config, allowHost, slog.New(slog.DiscardHandler))
}

func TestProbeRejectsABadRequest(t *testing.T) {
	config := &Config{
		Timeout: 5 * time.Second, RequestTimeout: 5 * time.Second, MaxInFlight: 4,
		AllowedHosts: []string{"127.0.0.1"},
		Modules:      map[string]Module{"default": {}},
	}
	handler := testHandler(t, config)

	testCases := []struct {
		name   string
		query  string
		status int
	}{
		{"no target", "/probe", http.StatusBadRequest},
		{"a target that is not a URL", "/probe?target=%3A%3A%3A", http.StatusBadRequest},
		{"a target that is not http", "/probe?target=file:///etc/passwd", http.StatusBadRequest},
		{"a host outside allowed_hosts", "/probe?target=https://example.com/f.pb", http.StatusForbidden},
		{"an unknown module", "/probe?target=http://127.0.0.1/f.pb&module=absent", http.StatusBadRequest},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler(recorder, httptest.NewRequest(http.MethodGet, testCase.query, nil))
			if recorder.Code != testCase.status {
				t.Errorf("got status %d, want %d", recorder.Code, testCase.status)
			}
		})
	}
}

// TestProbeSetsNoTargetLabel checks that the exporter never names the network.
// Prometheus owns the target identity through relabelling.
func TestProbeSetsNoTargetLabel(t *testing.T) {
	server := testFeedServer(t)
	config := &Config{
		Timeout: 5 * time.Second, RequestTimeout: 5 * time.Second, MaxInFlight: 4,
		Modules: map[string]Module{"default": {}},
	}
	recorder := httptest.NewRecorder()
	testHandler(t, config)(recorder, httptest.NewRequest(http.MethodGet, "/probe?target="+server.URL, nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200", recorder.Code)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "gtfs_rt_up 1") {
		t.Errorf("the body must report the feed as up, got:\n%s", body)
	}
	for _, forbidden := range []string{"target=", "network=", "system=", "instance="} {
		if strings.Contains(body, forbidden) {
			t.Errorf("the body must carry no %s label, got:\n%s", forbidden, body)
		}
	}
}

// TestMaxInFlightRejectsTheExtraScrape checks that the cap holds. A cap inside
// the request would never fill.
func TestMaxInFlightRejectsTheExtraScrape(t *testing.T) {
	release := make(chan struct{})
	arrived := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		<-release
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	config := &Config{
		Timeout: 5 * time.Second, RequestTimeout: 5 * time.Second, MaxInFlight: 1,
		Modules: map[string]Module{"default": {}},
	}
	handler := testHandler(t, config)
	request := httptest.NewRequest(http.MethodGet, "/probe?target="+server.URL, nil)

	go handler(httptest.NewRecorder(), request)
	<-arrived // The first scrape now holds the only slot.

	recorder := httptest.NewRecorder()
	handler(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("got status %d, want 503", recorder.Code)
	}
}

func TestEndpointsAreSeparate(t *testing.T) {
	recorder := httptest.NewRecorder()
	indexHandler(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "/probe?target=") {
		t.Errorf("the landing page must explain the probe endpoint, got %d:\n%s", recorder.Code, recorder.Body.String())
	}

	notFound := httptest.NewRecorder()
	indexHandler(notFound, httptest.NewRequest(http.MethodGet, "/nothing", nil))
	if notFound.Code != http.StatusNotFound {
		t.Errorf("got status %d for an unknown path, want 404", notFound.Code)
	}
}

// TestPerRouteDelayNeedsPerRoute checks that the expensive family cannot be
// turned on alone.
//
// The per-route histogram carries only the route identifier. Without per_route
// there is no gtfs_static_route_info to join a name from, so eighteen series a
// route would arrive unreadable.
func TestPerRouteDelayNeedsPerRoute(t *testing.T) {
	if _, err := loadConfig(writeConfig(t, "modules:\n  m:\n    per_route_delay: true\n")); err == nil {
		t.Error("got no error for per_route_delay without per_route, want one")
	}
	if _, err := loadConfig(writeConfig(t,
		"modules:\n  m:\n    per_route: true\n    per_route_delay: true\n")); err != nil {
		t.Errorf("got error %v with both set, want none", err)
	}
	if _, err := loadConfig(writeConfig(t,
		"modules:\n  m:\n    per_route: true\n    late_threshold: -1s\n")); err == nil {
		t.Error("got no error for a negative late_threshold, want one")
	}
}
