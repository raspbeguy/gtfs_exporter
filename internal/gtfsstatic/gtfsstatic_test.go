package gtfsstatic_test

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raspbeguy/gtfs_exporter/internal/gtfsstatic"
)

// members of the fixture, in the order that Rennes uses. shapes.txt sits
// between routes.txt and trips.txt, so a reader that fetches one span across
// the three needed members would pull it in as well.
func fixture(t *testing.T, agency, routes, trips, feedInfo string, filler int) []byte {
	t.Helper()
	buffer := &bytes.Buffer{}
	writer := zip.NewWriter(buffer)
	add := func(name, body string) {
		file, err := writer.Create(name)
		if err != nil {
			t.Fatalf("cannot create %s: %v", name, err)
		}
		if _, err := file.Write([]byte(body)); err != nil {
			t.Fatalf("cannot write %s: %v", name, err)
		}
	}
	add("agency.txt", agency)
	add("routes.txt", routes)
	// The filler stands for shapes.txt. Random-looking text keeps it from
	// compressing away to nothing.
	if filler > 0 {
		var big strings.Builder
		for i := 0; big.Len() < filler; i++ {
			fmt.Fprintf(&big, "%d,%f,%f,%d\n", i, float64(i)*1.7182818, float64(i)*3.1415926, i*7919)
		}
		add("shapes.txt", big.String())
		add("stop_times.txt", big.String())
	}
	add("trips.txt", trips)
	if feedInfo != "" {
		add("feed_info.txt", feedInfo)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("cannot close the zip: %v", err)
	}
	return buffer.Bytes()
}

const (
	oneAgency = "agency_id,agency_name,agency_timezone\nSTAR,Star,Europe/Paris\n"
	// routes.txt of a feed with one agency omits agency_id, which GTFS allows.
	routesNoAgency = "route_id,route_short_name,route_long_name,route_type\n" +
		"6-1001,a,Kennedy to La Poterie,1\n6-0003,3,A line,3\n"
	someTrips = "route_id,service_id,trip_id\n6-1001,s1,t1\n6-1001,s1,t2\n6-0003,s1,t3\n"
	someInfo  = "feed_publisher_name,feed_version,feed_start_date,feed_end_date\nStar,2026-08-21,20260821,20260930\n"
)

// rangeServer serves a body and records the byte spans that it was asked for.
type rangeServer struct {
	body        []byte
	etag        string
	ignoreRange bool
	status      int

	mutex    sync.Mutex
	spans    [][2]int64
	requests int
}

func (s *rangeServer) served(from, to int64) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for _, span := range s.spans {
		if span[0] <= to && from <= span[1] {
			return true
		}
	}
	return false
}

func (s *rangeServer) count() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.requests
}

func (s *rangeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mutex.Lock()
	s.requests++
	s.mutex.Unlock()

	if s.status != 0 {
		http.Error(w, "no", s.status)
		return
	}
	if s.etag != "" {
		if r.Header.Get("If-None-Match") == s.etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", s.etag)
	}
	size := int64(len(s.body))
	header := r.Header.Get("Range")
	if s.ignoreRange || header == "" {
		s.record(0, size-1)
		_, _ = w.Write(s.body)
		return
	}
	from, to, ok := parseRange(header, size)
	if !ok {
		http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	s.record(from, to)
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, to, size))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(s.body[from : to+1])
}

func (s *rangeServer) record(from, to int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.spans = append(s.spans, [2]int64{from, to})
}

func parseRange(header string, size int64) (from, to int64, ok bool) {
	spec, found := strings.CutPrefix(header, "bytes=")
	if !found {
		return 0, 0, false
	}
	first, last, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false
	}
	if first == "" { // A suffix range, such as bytes=-65536.
		length, err := strconv.ParseInt(last, 10, 64)
		if err != nil {
			return 0, 0, false
		}
		if length > size {
			length = size
		}
		return size - length, size - 1, true
	}
	from, err := strconv.ParseInt(first, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	to = size - 1
	if last != "" {
		if to, err = strconv.ParseInt(last, 10, 64); err != nil {
			return 0, 0, false
		}
	}
	if to >= size {
		to = size - 1
	}
	return from, to, from <= to
}

func loadOnce(t *testing.T, server *rangeServer) (*gtfsstatic.Loader, *httptest.Server) {
	t.Helper()
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	loader := gtfsstatic.NewLoader(
		gtfsstatic.Feed{URL: httpServer.URL, RefreshInterval: time.Hour},
		httpServer.Client(), slog.New(slog.DiscardHandler))
	loader.Start(t.Context())
	waitForIndex(t, loader)
	return loader, httpServer
}

func waitForIndex(t *testing.T, loader *gtfsstatic.Loader) *gtfsstatic.Index {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if index := loader.Index(); index != nil {
			return index
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the index never became ready")
	return nil
}

// TestOnlyTheNeededMembersAreRead is the invariant that makes static
// enrichment possible at all.
//
// Ile-de-France holds a 271 MB zip whose stop_times.txt is 1.5 GB uncompressed
// and whose shapes.txt is 991 MB. The exporter needs 3.96 MB of it. The test
// puts a large member between routes.txt and trips.txt, the way Rennes does,
// and proves that its bytes are never asked for.
func TestOnlyTheNeededMembersAreRead(t *testing.T) {
	body := fixture(t, oneAgency, routesNoAgency, someTrips, someInfo, 4<<20)
	server := &rangeServer{body: body}
	loader, _ := loadOnce(t, server)

	index := loader.Index()
	agencies, routes, trips := index.Stats()
	if agencies != 1 || routes != 2 || trips != 3 {
		t.Errorf("got %d agencies, %d routes and %d trips, want 1, 2 and 3", agencies, routes, trips)
	}

	// The loader reads the tail of the file to find the central directory, and
	// it reads whole blocks. Both clip whatever member happens to sit at the
	// edge, so the invariant is about the body of the big members and not
	// about every last byte of them.
	const edge = 256 << 10
	reader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("cannot read the fixture: %v", err)
	}
	for _, file := range reader.File {
		if file.Name != "shapes.txt" && file.Name != "stop_times.txt" {
			continue
		}
		offset, err := file.DataOffset()
		if err != nil {
			t.Fatalf("cannot find %s: %v", file.Name, err)
		}
		from := offset + edge
		to := offset + int64(file.CompressedSize64) - edge
		if to > int64(len(body))-edge {
			to = int64(len(body)) - edge
		}
		if to <= from {
			t.Fatalf("the fixture member %s is too small to test", file.Name)
		}
		if server.served(from, to) {
			t.Errorf("the loader fetched the body of %s, which it must never read", file.Name)
		}
	}

	// The saving is the point. Ile-de-France is 271 MB and the exporter needs
	// 3.96 MB of it.
	if index.Bytes() > int64(len(body))/4 {
		t.Errorf("the loader read %d bytes of a %d byte zip, want at most a quarter",
			index.Bytes(), len(body))
	}
}

// TestTheRangeReaderFallsBackToAFullDownload checks the other path. A server
// that ignores the range header sends the whole file, and the loader must
// still build an index.
func TestTheRangeReaderFallsBackToAFullDownload(t *testing.T) {
	server := &rangeServer{
		body:        fixture(t, oneAgency, routesNoAgency, someTrips, someInfo, 0),
		ignoreRange: true,
	}
	loader, _ := loadOnce(t, server)

	if _, _, trips := loader.Index().Stats(); trips != 3 {
		t.Errorf("got %d trips, want 3", trips)
	}
	if route, ok := loader.Index().RouteOfTrip("t3"); !ok || route.ID != "6-0003" {
		t.Errorf("got route %+v and %v, want 6-0003", route, ok)
	}
}

// TestRoutesWithoutAgencyIDUseTheOnlyAgency checks a GTFS rule that is easy to
// miss.
//
// agency_id is optional in routes.txt when the feed holds exactly one agency.
// Rennes is such a feed. A reader that takes the column at face value gives
// every route an empty agency label.
func TestRoutesWithoutAgencyIDUseTheOnlyAgency(t *testing.T) {
	server := &rangeServer{body: fixture(t, oneAgency, routesNoAgency, someTrips, someInfo, 0)}
	loader, _ := loadOnce(t, server)

	route, ok := loader.Index().Route("6-1001")
	if !ok {
		t.Fatal("the route must be in the index")
	}
	if route.AgencyID != "STAR" {
		t.Errorf("got agency %q, want STAR", route.AgencyID)
	}
	if route.ShortName != "a" || route.Type != "1" {
		t.Errorf("got short name %q and type %q, want a and 1", route.ShortName, route.Type)
	}
}

// TestTwoAgenciesLeaveAnEmptyAgencyEmpty checks the other half of the rule.
// With more than one agency, GTFS requires the column, and guessing would be
// wrong.
func TestTwoAgenciesLeaveAnEmptyAgencyEmpty(t *testing.T) {
	two := "agency_id,agency_name,agency_timezone\nA,First,Europe/Paris\nB,Second,Europe/Paris\n"
	server := &rangeServer{body: fixture(t, two, routesNoAgency, someTrips, someInfo, 0)}
	loader, _ := loadOnce(t, server)

	route, _ := loader.Index().Route("6-1001")
	if route.AgencyID != "" {
		t.Errorf("got agency %q, want an empty one", route.AgencyID)
	}
}

// TestTheFeedInfoNamesTheDataset checks that the loader reads the fourth
// member. Without it a reader cannot tell which dataset produced a mismatch.
func TestTheFeedInfoNamesTheDataset(t *testing.T) {
	server := &rangeServer{body: fixture(t, oneAgency, routesNoAgency, someTrips, someInfo, 0)}
	loader, _ := loadOnce(t, server)

	info := loader.Index().FeedInfo()
	if info.Version != "2026-08-21" || info.StartDate != "20260821" || info.EndDate != "20260930" {
		t.Errorf("got %+v, want the dates of the fixture", info)
	}
}

// TestAFeedWithoutFeedInfoStillLoads checks that the optional member stays
// optional.
func TestAFeedWithoutFeedInfoStillLoads(t *testing.T) {
	server := &rangeServer{body: fixture(t, oneAgency, routesNoAgency, someTrips, "", 0)}
	loader, _ := loadOnce(t, server)

	if _, _, trips := loader.Index().Stats(); trips != 3 {
		t.Errorf("got %d trips, want 3", trips)
	}
	if version := loader.Index().FeedInfo().Version; version != "" {
		t.Errorf("got version %q, want an empty one", version)
	}
}

// TestARefreshFailureKeepsTheOldIndex is the rule that keeps the labels of
// every metric alive through an outage.
//
// A stale route name beats no route name. A feed that answers badly for an
// hour must never empty the index.
func TestARefreshFailureKeepsTheOldIndex(t *testing.T) {
	server := &rangeServer{body: fixture(t, oneAgency, routesNoAgency, someTrips, someInfo, 0)}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	loader := gtfsstatic.NewLoader(
		gtfsstatic.Feed{URL: httpServer.URL, RefreshInterval: 20 * time.Millisecond},
		httpServer.Client(), slog.New(slog.DiscardHandler))
	loader.Start(t.Context())
	first := waitForIndex(t, loader)

	// Break the server and wait for at least one refresh to fail.
	server.mutex.Lock()
	server.status = http.StatusInternalServerError
	server.mutex.Unlock()

	deadline := time.Now().Add(10 * time.Second)
	for loader.Failures() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if loader.Failures() == 0 {
		t.Fatal("no refresh failed, so the rule was never exercised")
	}
	if loader.Index() != first {
		t.Error("a failed refresh must keep the index that is in place")
	}
	if route, ok := loader.Index().Route("6-1001"); !ok || route.ShortName != "a" {
		t.Error("the old index must still answer")
	}
}

// TestAConditionalRequestSkipsAnUnchangedFeed checks why a refresh is cheap.
// The static feed of data.gouv.fr serves an ETag, so most refreshes of a 271
// MB file cost one round trip.
func TestAConditionalRequestSkipsAnUnchangedFeed(t *testing.T) {
	server := &rangeServer{
		body: fixture(t, oneAgency, routesNoAgency, someTrips, someInfo, 0),
		etag: `"v1"`,
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	loader := gtfsstatic.NewLoader(
		gtfsstatic.Feed{URL: httpServer.URL, RefreshInterval: 10 * time.Millisecond},
		httpServer.Client(), slog.New(slog.DiscardHandler))
	loader.Start(t.Context())
	first := waitForIndex(t, loader)

	before := server.count()
	deadline := time.Now().Add(2 * time.Second)
	for server.count() < before+3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.count() < before+3 {
		t.Fatal("the loader did not refresh")
	}
	if loader.Index() != first {
		t.Error("an unchanged feed must not rebuild the index")
	}
	if loader.Failures() != 0 {
		t.Errorf("got %v failures, want 0 for a 304 answer", loader.Failures())
	}
}

// TestOneLoaderIsSharedByTheTargetsOfOneNetwork checks the registry.
//
// A network publishes up to three realtime feeds and one static feed. Each
// realtime feed is its own target, so without sharing the exporter would read
// the same 271 MB zip three times.
func TestOneLoaderIsSharedByTheTargetsOfOneNetwork(t *testing.T) {
	var loads atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		loads.Add(1)
		(&rangeServer{body: fixture(t, oneAgency, routesNoAgency, someTrips, someInfo, 0)}).ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)

	registry := gtfsstatic.NewRegistry(t.Context(), server.Client(), slog.New(slog.DiscardHandler))
	feed := gtfsstatic.Feed{URL: server.URL, RefreshInterval: time.Hour}

	first := registry.Loader(feed)
	second := registry.Loader(feed)
	third := registry.Loader(feed)
	if first != second || second != third {
		t.Error("the three targets of one network must share one loader")
	}
	waitForIndex(t, first)
}

// TestAnIndexThatIsNotReadyAnswersEverything checks the nil paths. A module
// with no static feed holds a nil loader, and a loader whose first read is
// still running holds a nil index. Neither may end a scrape.
func TestAnIndexThatIsNotReadyAnswersEverything(t *testing.T) {
	var absent *gtfsstatic.Loader
	if index := absent.Index(); index != nil {
		t.Error("a nil loader must hold no index")
	}
	if absent.Failures() != 0 || absent.URL() != "" {
		t.Error("a nil loader must answer with zero values")
	}

	var none *gtfsstatic.Index
	if _, ok := none.RouteOfTrip("t1"); ok {
		t.Error("a nil index must find no trip")
	}
	if _, ok := none.Route("r1"); ok {
		t.Error("a nil index must find no route")
	}
	agencies, routes, trips := none.Stats()
	if agencies != 0 || routes != 0 || trips != 0 {
		t.Error("a nil index must be empty")
	}
	if none.FeedInfo().Version != "" || !none.LoadedAt().IsZero() || none.Bytes() != 0 {
		t.Error("a nil index must answer with zero values")
	}
}

// TestALoadIsBoundedByItsContext checks that the loader stops with the
// process rather than holding a request open.
func TestALoadIsBoundedByItsContext(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	ctx, cancel := context.WithCancel(context.Background())
	loader := gtfsstatic.NewLoader(
		gtfsstatic.Feed{URL: server.URL, RefreshInterval: time.Hour},
		server.Client(), slog.New(slog.DiscardHandler))
	loader.Start(ctx)
	cancel()

	deadline := time.Now().Add(5 * time.Second)
	for loader.Failures() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if loader.Failures() == 0 {
		t.Error("the cancelled load must end and be counted")
	}
	if loader.Index() != nil {
		t.Error("a load that never finished must install no index")
	}
}

// TestOneURLWithTwoScheduleSettingsGetsTwoLoaders guards a collision.
//
// Two modules can name the same zip and ask for different things from it. If
// the registry keyed on the URL alone, whichever module asked first would
// decide for both, and a module with schedule set could silently receive an
// index with no timetable and publish no delay at all.
func TestOneURLWithTwoScheduleSettingsGetsTwoLoaders(t *testing.T) {
	server := httptest.NewServer(&rangeServer{
		body: fixture(t, oneAgency, routesNoAgency, someTrips, someInfo, 0),
	})
	t.Cleanup(server.Close)

	registry := gtfsstatic.NewRegistry(t.Context(), server.Client(), slog.New(slog.DiscardHandler))
	plain := gtfsstatic.Feed{URL: server.URL, RefreshInterval: time.Hour}
	timed := gtfsstatic.Feed{URL: server.URL, RefreshInterval: time.Hour, Schedule: true}

	if registry.Loader(plain) == registry.Loader(timed) {
		t.Fatal("a module that asks for a timetable must not share the index of one that does not")
	}
	// The same request twice still shares, which is the point of the registry.
	if registry.Loader(timed) != registry.Loader(timed) {
		t.Error("two modules with the same settings must share one loader")
	}
}
