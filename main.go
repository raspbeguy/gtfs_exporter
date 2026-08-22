// Command gtfs_exporter serves GTFS-Realtime feed data as Prometheus metrics.
//
// The exporter follows the multi-target pattern of blackbox_exporter. One
// scrape of /probe reads one feed URL, and the configuration file holds no
// target.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	versioncollector "github.com/prometheus/client_golang/prometheus/collectors/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	commonversion "github.com/prometheus/common/version"
	"gopkg.in/yaml.v3"

	"github.com/raspbeguy/gtfs_exporter/internal/collector"
	"github.com/raspbeguy/gtfs_exporter/internal/gtfsrt"
	"github.com/raspbeguy/gtfs_exporter/internal/gtfsstatic"
)

// version is the build version. The release workflow overrides it with
// -ldflags "-X main.version=...".
var version = "dev"

// rejected counts the requests that the exporter refused. Such a request
// carries no metrics body, so Prometheus sees only that the scrape failed and
// never why. The counter lives on the self endpoint, because it counts the
// work of the exporter and not the state of one feed.
var rejected = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "gtfs_exporter_requests_rejected_total",
	Help: "Requests that the exporter refused, by reason.",
}, []string{"reason"})

// Config is the content of the configuration file. It holds no target. The
// target arrives in the query string of each scrape.
type Config struct {
	ListenAddress string `yaml:"listen_address"`
	// Timeout is the budget for one scrape. A scrape that carries the header
	// X-Prometheus-Scrape-Timeout-Seconds uses the lower of the two.
	Timeout time.Duration `yaml:"timeout"`
	// RequestTimeout is the budget for the HTTP read of one feed. Keep it
	// below Timeout.
	RequestTimeout time.Duration `yaml:"request_timeout"`
	UserAgent      string        `yaml:"user_agent"`
	// AllowedHosts lists the hosts that the exporter accepts as a target. An
	// empty list accepts every host. The list is tested again on every
	// redirect hop.
	AllowedHosts []string `yaml:"allowed_hosts"`
	// MaxInFlight limits the number of scrapes that run at the same time.
	MaxInFlight int `yaml:"max_in_flight"`
	// Modules hold the settings that a query string must not carry, such as
	// an API key. The module parameter of a request names one of them.
	Modules map[string]Module `yaml:"modules"`
}

// Module holds the read settings of one operator.
type Module struct {
	// PerRoute adds one series per route to the trip-update family. Ile-de-
	// France publishes 1973 routes, so measure a network before you turn this
	// on.
	PerRoute bool `yaml:"per_route"`
	// PerRouteDelay adds the delay histogram per route, which is eighteen
	// series a route: about 880 for Rennes and 8,400 for MBTA. It needs
	// PerRoute, because the histogram carries only the identifier and the name
	// comes from gtfs_static_route_info.
	PerRouteDelay bool `yaml:"per_route_delay"`
	// LateThreshold is the delay past which a trip counts as late. A metro and
	// a regional train do not agree on what late means. The default is 5m.
	LateThreshold time.Duration `yaml:"late_threshold"`
	// Headers go with the request. Use them for an API key or for a client
	// name that the operator asks for.
	Headers map[string]string `yaml:"headers"`
	// RequestTimeout is the budget for the feed of this operator. It overrides
	// the request_timeout of the file.
	RequestTimeout time.Duration `yaml:"request_timeout"`
	// ReuseConnections keeps the connection alive between scrapes. The default
	// is true. Set it to false for an operator that serves the first request
	// of a connection at once and then holds every later one.
	ReuseConnections *bool `yaml:"reuse_connections"`
	// DelayBuckets overrides the classic histogram buckets of the trip delay,
	// in seconds. Delays run from -493 to 7800 seconds in the feeds that this
	// exporter was tested against.
	DelayBuckets []float64 `yaml:"delay_buckets"`
	// NativeHistogram publishes the trip delay as a native histogram instead
	// of a classic one. A metric is one or the other, never both, so a
	// scraper without native-histogram support then sees only the sum and the
	// count. The measured median delay is 0, which is where the zero bucket
	// of a native histogram loses resolution, so the default is false.
	NativeHistogram bool `yaml:"native_histogram"`
	// StaticGTFS names the static feed of this network. It fills the route of
	// a trip for a feed that publishes none, and it names the dataset. The
	// three realtime feeds of one network must name the same static feed, so
	// that they share one index rather than reading it three times.
	StaticGTFS *StaticFeed `yaml:"static_gtfs"`
}

// StaticFeed is the static GTFS zip of one network.
type StaticFeed struct {
	URL     string            `yaml:"url"`
	Headers map[string]string `yaml:"headers"`
	// Schedule reads stop_times.txt and builds a timetable, so that the
	// exporter can compute a delay for a feed that publishes only absolute
	// times. Rennes and MBTA both need it. It is off by default, because that
	// member is the largest of the zip and the result stays in memory: about
	// 19 MB for Rennes and 39 MB for MBTA.
	Schedule bool `yaml:"schedule"`
	// MaxScheduleRows refuses a network whose timetable would not fit. The
	// route index still loads, because it is small and useful on its own.
	MaxScheduleRows int `yaml:"schedule_max_rows"`
	// RenewBefore starts renewing this long before the dataset expires. A
	// dataset lives one to two weeks, so the margin matters more than the
	// interval.
	RenewBefore time.Duration `yaml:"renew_before"`
	// RenewInterval is the retry gap inside that margin, until a later dataset
	// arrives. A conditional request answers 304, so it is cheap.
	RenewInterval time.Duration `yaml:"renew_interval"`
	// RefreshInterval is the gap between two loads. The default is 12h. A
	// conditional request makes most refreshes cost one round trip.
	RefreshInterval time.Duration `yaml:"refresh_interval"`
	// MaxBytes limits the size of the zip. Ile-de-France is 271 MB, and the
	// exporter reads 3.96 MB of it with range requests.
	MaxBytes int64 `yaml:"max_bytes"`
}

func main() {
	configPath := flag.String("config", "config.yml", "Path of the configuration file.")
	listenAddress := flag.String("web.listen-address", "", "Address to listen on. This flag overrides the configuration file.")
	logLevel := flag.String("log.level", "info", "Log level: debug, info, warn, or error.")
	showVersion := flag.Bool("version", false, "Print the version and exit.")
	flag.Parse()

	if *showVersion {
		fmt.Println("gtfs_exporter", version)
		return
	}

	log := newLogger(*logLevel)

	config, err := loadConfig(*configPath)
	if err != nil {
		log.Error("cannot load the configuration", "path", *configPath, "error", err)
		os.Exit(1)
	}
	if *listenAddress != "" {
		config.ListenAddress = *listenAddress
	}

	allowed := make(map[string]bool, len(config.AllowedHosts))
	for _, host := range config.AllowedHosts {
		allowed[strings.ToLower(host)] = true
	}
	allowHost := func(host string) bool {
		return len(allowed) == 0 || allowed[strings.ToLower(host)]
	}
	if len(allowed) == 0 {
		log.Warn("allowed_hosts is empty, so the exporter accepts every host as a target. " +
			"A caller who reaches this port can read any address that the exporter can reach. " +
			"Set allowed_hosts, or keep the port closed to untrusted callers.")
	}

	client := gtfsrt.NewClient(config.RequestTimeout, config.UserAgent, allowHost)

	// The static loaders run in the background for the life of the process. A
	// scrape never waits for one: reading the Ile-de-France dataset takes
	// about fifteen seconds.
	loaderCtx, stopLoaders := context.WithCancel(context.Background())
	defer stopLoaders()
	static := gtfsstatic.NewRegistry(loaderCtx, &http.Client{
		Transport: &http.Transport{Proxy: http.ProxyFromEnvironment},
	}, log)

	// The target endpoint and the self endpoint are separate, the layout that
	// blackbox_exporter and snmp_exporter use. A scrape of one feed must not
	// carry the runtime metrics of the exporter, because they would repeat
	// under the instance label of every target.
	commonversion.Version = version

	self := prometheus.NewRegistry()
	self.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		versioncollector.NewCollector("gtfs_exporter"),
		rejected,
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/probe", probeHandler(client, static, config, allowHost, log))
	mux.Handle("/metrics", promhttp.HandlerFor(self, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
		Registry:      self,
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/", indexHandler)

	server := &http.Server{
		Addr:              config.ListenAddress,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Info("listening", "address", config.ListenAddress, "modules", len(config.Modules))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("the server stopped", "error", err)
			os.Exit(1)
		}
	}()

	<-stop
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

// probeHandler reads the one feed that the query string names.
//
// The query parameters are target and module. The configuration file holds no
// target, so Prometheus owns the list of feeds, and the person who runs
// Prometheus sets the network label on the target.
//
// The endpoint fetches the URL that the caller gives. Set allowed_hosts, or
// keep the port closed to untrusted callers.
func probeHandler(client *gtfsrt.Client, static *gtfsstatic.Registry, config *Config, allowHost func(string) bool, log *slog.Logger) http.HandlerFunc {
	// inFlight caps the scrapes that run at the same time. The cap must live
	// outside the request, because a counter inside the request never fills.
	inFlight := make(chan struct{}, config.MaxInFlight)

	return func(w http.ResponseWriter, r *http.Request) {
		target := r.URL.Query().Get("target")
		if target == "" {
			rejected.WithLabelValues("bad_target").Inc()
			http.Error(w, "the target parameter is missing", http.StatusBadRequest)
			return
		}
		parsed, err := url.Parse(target)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			rejected.WithLabelValues("bad_target").Inc()
			http.Error(w, "the target parameter must be an http or https URL", http.StatusBadRequest)
			return
		}
		if !allowHost(parsed.Hostname()) {
			rejected.WithLabelValues("forbidden_host").Inc()
			log.Warn("the target is not in allowed_hosts", "host", parsed.Hostname())
			http.Error(w, "the host of the target is not in allowed_hosts", http.StatusForbidden)
			return
		}

		module, err := selectModule(config.Modules, r.URL.Query().Get("module"))
		if err != nil {
			rejected.WithLabelValues("unknown_module").Inc()
			log.Warn("the request names a module that the configuration does not hold",
				"module", r.URL.Query().Get("module"))
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		select {
		case inFlight <- struct{}{}:
			defer func() { <-inFlight }()
		default:
			rejected.WithLabelValues("too_many_in_flight").Inc()
			http.Error(w, "too many scrapes run at the same time", http.StatusServiceUnavailable)
			return
		}

		var loader *gtfsstatic.Loader
		if module.StaticGTFS != nil {
			loader = static.Loader(gtfsstatic.Feed{
				URL:             module.StaticGTFS.URL,
				Headers:         module.StaticGTFS.Headers,
				RefreshInterval: module.StaticGTFS.RefreshInterval,
				MaxBytes:        module.StaticGTFS.MaxBytes,
				Schedule:        module.StaticGTFS.Schedule,
				MaxScheduleRows: module.StaticGTFS.MaxScheduleRows,
				RenewBefore:     module.StaticGTFS.RenewBefore,
				RenewInterval:   module.StaticGTFS.RenewInterval,
			})
		}

		feed := collector.Feed{
			// Name reaches the log only. The network label belongs to
			// Prometheus, which sets it on the target.
			Name:            parsed.Host,
			URL:             target,
			PerRoute:        module.PerRoute,
			Headers:         module.Headers,
			RequestTimeout:  module.RequestTimeout,
			NoKeepAlive:     module.ReuseConnections != nil && !*module.ReuseConnections,
			DelayBuckets:    module.DelayBuckets,
			NativeHistogram: module.NativeHistogram,
			Schedule:        module.StaticGTFS != nil && module.StaticGTFS.Schedule,
			PerRouteDelay:   module.PerRouteDelay,
			LateThreshold:   module.LateThreshold,
			Static:          loader,
		}
		registry := prometheus.NewRegistry()
		single := collector.New(client, feed, scrapeBudget(r, config.Timeout), log)
		// The request context stops the outbound request when Prometheus
		// gives up on the scrape.
		registry.MustRegister(single.WithContext(r.Context()))
		promhttp.HandlerFor(registry, promhttp.HandlerOpts{
			ErrorHandling: promhttp.ContinueOnError,
		}).ServeHTTP(w, r)
	}
}

// scrapeBudget returns the budget for one scrape.
//
// Prometheus sends X-Prometheus-Scrape-Timeout-Seconds on every scrape. A
// budget above that value only spends work on a request that Prometheus has
// already abandoned, so the lower of the two wins. blackbox_exporter reads the
// same header.
func scrapeBudget(r *http.Request, configured time.Duration) time.Duration {
	raw := r.Header.Get("X-Prometheus-Scrape-Timeout-Seconds")
	if raw == "" {
		return configured
	}
	seconds, err := strconv.ParseFloat(raw, 64)
	if err != nil || seconds <= 0 {
		return configured
	}
	fromHeader := time.Duration(seconds * float64(time.Second))
	if fromHeader < configured {
		return fromHeader
	}
	return configured
}

// selectModule returns the module that the name selects. An empty name
// selects the module called default, and the zero value if the configuration
// holds no such module.
func selectModule(modules map[string]Module, name string) (Module, error) {
	if name == "" {
		return modules["default"], nil
	}
	module, ok := modules[name]
	if !ok {
		return Module{}, fmt.Errorf("the configuration holds no module %q", name)
	}
	return module, nil
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprintf(w, `<html>
<head><title>GTFS-Realtime exporter</title></head>
<body>
<h1>GTFS-Realtime exporter %s</h1>
<p>Read one feed with /probe?target=&lt;URL of the GTFS-Realtime feed&gt;</p>
<p>Add &amp;module=&lt;name&gt; for the settings of an operator.</p>
<p><a href="/metrics">Metrics of the exporter itself</a></p>
</body>
</html>
`, version)
}

func loadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	config := &Config{}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	// An empty file gives io.EOF. Every setting has a default, so an empty
	// file is valid.
	if err := decoder.Decode(config); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	if config.ListenAddress == "" {
		config.ListenAddress = ":10053"
	}
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 10 * time.Second
	}
	if config.RequestTimeout > config.Timeout {
		return nil, errors.New("request_timeout is higher than timeout")
	}
	if config.MaxInFlight <= 0 {
		config.MaxInFlight = 4
	}
	if config.UserAgent == "" {
		config.UserAgent = "gtfs_exporter/" + version
	}
	for name, module := range config.Modules {
		if err := validateBuckets(module.DelayBuckets); err != nil {
			return nil, fmt.Errorf("module %q: %w", name, err)
		}
		// The per-route histogram carries only the route identifier. Without
		// per_route there is no gtfs_static_route_info to join a name from, so
		// eighteen series a route would arrive unreadable.
		if module.PerRouteDelay && !module.PerRoute {
			return nil, fmt.Errorf("module %q: per_route_delay needs per_route", name)
		}
		if module.LateThreshold < 0 {
			return nil, fmt.Errorf("module %q: late_threshold cannot be negative", name)
		}
		if module.StaticGTFS == nil {
			continue
		}
		// A static feed with no address is a typo that would otherwise fail
		// silently in the background, long after the exporter started.
		parsed, err := url.Parse(module.StaticGTFS.URL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return nil, fmt.Errorf("module %q: static_gtfs.url must be an http or https URL", name)
		}
	}
	return config, nil
}

// validateBuckets rejects a bucket list that Prometheus cannot use. A
// histogram needs strictly increasing bounds.
func validateBuckets(buckets []float64) error {
	for i := 1; i < len(buckets); i++ {
		if buckets[i] <= buckets[i-1] {
			return fmt.Errorf("delay_buckets must increase, but %g follows %g", buckets[i], buckets[i-1])
		}
	}
	return nil
}

func newLogger(level string) *slog.Logger {
	var parsed slog.Level
	if err := parsed.UnmarshalText([]byte(level)); err != nil {
		parsed = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: parsed}))
}
