package collector_test

import (
	"archive/zip"
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"

	"github.com/raspbeguy/gtfs_exporter/internal/collector"
	"github.com/raspbeguy/gtfs_exporter/internal/gtfsrt"
	"github.com/raspbeguy/gtfs_exporter/internal/gtfsstatic"
)

// serveFeed answers every request with the given feed message.
func serveFeed(t *testing.T, message *gtfs.FeedMessage) *httptest.Server {
	t.Helper()
	body, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("cannot marshal the fixture: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	return server
}

func newCollector(t *testing.T, url string, feed collector.Feed) prometheus.Collector {
	t.Helper()
	feed.URL = url
	feed.Name = "test"
	client := gtfsrt.NewClient(5*time.Second, "test", nil)
	return collector.New(client, feed, 5*time.Second, slog.New(slog.DiscardHandler)).
		WithContext(context.Background())
}

func header() *gtfs.FeedHeader {
	return &gtfs.FeedHeader{
		GtfsRealtimeVersion: proto.String("2.0"),
		Incrementality:      gtfs.FeedHeader_FULL_DATASET.Enum(),
		Timestamp:           proto.Uint64(1787412524),
	}
}

func entity(id string, payload any) *gtfs.FeedEntity {
	e := &gtfs.FeedEntity{Id: proto.String(id)}
	switch value := payload.(type) {
	case *gtfs.TripUpdate:
		e.TripUpdate = value
	case *gtfs.VehiclePosition:
		e.Vehicle = value
	case *gtfs.Alert:
		e.Alert = value
	}
	return e
}

// stopTime builds one stop time update. A nil delay leaves the field unset,
// which is what Rennes and MBTA publish.
func stopTime(sequence uint32, delay *int32, at *int64, scheduled *int64,
	relationship gtfs.TripUpdate_StopTimeUpdate_ScheduleRelationship,
) *gtfs.TripUpdate_StopTimeUpdate {
	return &gtfs.TripUpdate_StopTimeUpdate{
		StopSequence:         proto.Uint32(sequence),
		ScheduleRelationship: relationship.Enum(),
		Departure: &gtfs.TripUpdate_StopTimeEvent{
			Delay:         delay,
			Time:          at,
			ScheduledTime: scheduled,
		},
	}
}

func future() *int64 {
	value := time.Now().Add(10 * time.Minute).Unix()
	return &value
}

// TestAHeaderTimestampInTheFutureIsNotClamped checks that the exporter writes
// the header time as the producer wrote it.
//
// Four of the thirty feeds that this exporter was tested against publish a
// time ahead of the clock of the reader, by up to 77 seconds. A clamp would
// hide the skew that the operator needs to see.
func TestAHeaderTimestampInTheFutureIsNotClamped(t *testing.T) {
	ahead := uint64(time.Now().Add(time.Hour).Unix())
	head := header()
	head.Timestamp = &ahead
	server := serveFeed(t, &gtfs.FeedMessage{Header: head})
	subject := newCollector(t, server.URL, collector.Feed{})

	got := testutil.ToFloat64(gaugeOf(t, subject, "gtfs_rt_feed_timestamp_seconds"))
	if got != float64(ahead) {
		t.Errorf("got %v, want %v", got, float64(ahead))
	}
}

// TestAnEmptyFeedIsUpWithZeroEntities checks a feed that answers with a valid
// header and no entity. Mulhouse Solea does this.
func TestAnEmptyFeedIsUpWithZeroEntities(t *testing.T) {
	server := serveFeed(t, &gtfs.FeedMessage{Header: header()})
	subject := newCollector(t, server.URL, collector.Feed{})

	expected := `
# HELP gtfs_rt_up 1 if the exporter fetched and decoded the feed, 0 if it failed. Alert on this metric, and not on a metric that disappears.
# TYPE gtfs_rt_up gauge
gtfs_rt_up 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_up"); err != nil {
		t.Error(err)
	}
	if count := testutil.CollectAndCount(subject, "gtfs_rt_entities"); count != 0 {
		t.Errorf("got %d entity series, want 0", count)
	}
}

// TestACorruptPayloadSetsUpToZero checks that a body which is not a feed
// message is visible as a failure and not as an empty feed.
func TestACorruptPayloadSetsUpToZero(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("this is not protobuf at all, not even close"))
	}))
	t.Cleanup(server.Close)
	subject := newCollector(t, server.URL, collector.Feed{})

	expected := `
# HELP gtfs_rt_up 1 if the exporter fetched and decoded the feed, 0 if it failed. Alert on this metric, and not on a metric that disappears.
# TYPE gtfs_rt_up gauge
gtfs_rt_up 0
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_up"); err != nil {
		t.Error(err)
	}
}

// TestAnOmittedOccupancyWritesNoSeries checks the absent-not-zero rule.
//
// Sixteen of the eighteen vehicle feeds that this exporter was tested against
// omit the occupancy field. A zero would claim an empty vehicle.
func TestAnOmittedOccupancyWritesNoSeries(t *testing.T) {
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("v1", &gtfs.VehiclePosition{
			CurrentStatus: gtfs.VehiclePosition_STOPPED_AT.Enum(),
			Position:      &gtfs.Position{Latitude: proto.Float32(48.1), Longitude: proto.Float32(-1.6)},
		}),
	}})
	subject := newCollector(t, server.URL, collector.Feed{})

	if count := testutil.CollectAndCount(subject, "gtfs_rt_vehicle_occupancy"); count != 0 {
		t.Errorf("got %d occupancy series, want 0", count)
	}
	if count := testutil.CollectAndCount(subject, "gtfs_rt_vehicles"); count != 1 {
		t.Errorf("got %d vehicle series, want 1", count)
	}
}

// TestPartialCoverageIsCounted checks that the coverage metric reports a count
// and not a ratio.
//
// Cannes publishes occupancy on 84 of its 85 vehicles, and MBTA on 355 of 473.
// A ratio would hide the vehicles that report nothing.
func TestPartialCoverageIsCounted(t *testing.T) {
	withOccupancy := &gtfs.VehiclePosition{
		CurrentStatus:   gtfs.VehiclePosition_IN_TRANSIT_TO.Enum(),
		OccupancyStatus: gtfs.VehiclePosition_FEW_SEATS_AVAILABLE.Enum(),
	}
	without := &gtfs.VehiclePosition{CurrentStatus: gtfs.VehiclePosition_IN_TRANSIT_TO.Enum()}
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("a", withOccupancy), entity("b", withOccupancy), entity("c", without),
	}})
	subject := newCollector(t, server.URL, collector.Feed{})

	expected := `
# HELP gtfs_rt_entities_with_field Entities that carry an optional field. A count and not a ratio, because coverage inside one feed is partial. Divide by gtfs_rt_entities for a ratio.
# TYPE gtfs_rt_entities_with_field gauge
gtfs_rt_entities_with_field{entity_type="vehicle_position",field="occupancy_status"} 2
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_entities_with_field"); err != nil {
		t.Error(err)
	}
}

// TestACanceledTripObservesNoDelay checks that a cancelled trip stays out of
// the histogram. A trip that does not run cannot be late.
func TestACanceledTripObservesNoDelay(t *testing.T) {
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("cancelled", &gtfs.TripUpdate{
			Trip: &gtfs.TripDescriptor{
				TripId:               proto.String("t1"),
				ScheduleRelationship: gtfs.TripDescriptor_CANCELED.Enum(),
			},
			Delay: proto.Int32(600),
		}),
		entity("running", &gtfs.TripUpdate{
			Trip:  &gtfs.TripDescriptor{TripId: proto.String("t2")},
			Delay: proto.Int32(60),
		}),
	}})
	subject := newCollector(t, server.URL, collector.Feed{})

	count, sum := histogramOf(t, subject, "gtfs_rt_trip_delay_seconds")
	if count != 1 || sum != 60 {
		t.Errorf("got count %d and sum %v, want 1 and 60", count, sum)
	}
}

// TestANoDataStopTimeUpdateObservesNoDelay checks that a stop which states no
// prediction never reaches the histogram. Clermont publishes 95 of them.
func TestANoDataStopTimeUpdateObservesNoDelay(t *testing.T) {
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("t", &gtfs.TripUpdate{
			Trip: &gtfs.TripDescriptor{TripId: proto.String("t1")},
			StopTimeUpdate: []*gtfs.TripUpdate_StopTimeUpdate{
				stopTime(1, proto.Int32(900), future(), nil, gtfs.TripUpdate_StopTimeUpdate_NO_DATA),
				stopTime(2, proto.Int32(120), future(), nil, gtfs.TripUpdate_StopTimeUpdate_SCHEDULED),
			},
		}),
	}})
	subject := newCollector(t, server.URL, collector.Feed{})

	count, sum := histogramOf(t, subject, "gtfs_rt_trip_delay_seconds")
	if count != 1 || sum != 120 {
		t.Errorf("got count %d and sum %v, want 1 and 120", count, sum)
	}
}

// TestScheduledTimeGivesADelayWithoutStaticGTFS checks the newer form of the
// stop time event, where the feed states the predicted time and the scheduled
// time and the reader subtracts them.
//
// No feed of the seven tested sets this field yet. It is the only way that a
// feed can state a delay without a static schedule, so the exporter reads it.
func TestScheduledTimeGivesADelayWithoutStaticGTFS(t *testing.T) {
	at := time.Now().Add(10 * time.Minute).Unix()
	scheduled := at - 240
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("t", &gtfs.TripUpdate{
			Trip: &gtfs.TripDescriptor{TripId: proto.String("t1")},
			StopTimeUpdate: []*gtfs.TripUpdate_StopTimeUpdate{
				stopTime(1, nil, &at, &scheduled, gtfs.TripUpdate_StopTimeUpdate_SCHEDULED),
			},
		}),
	}})
	subject := newCollector(t, server.URL, collector.Feed{})

	count, sum := histogramOf(t, subject, "gtfs_rt_trip_delay_seconds")
	if count != 1 || sum != 240 {
		t.Errorf("got count %d and sum %v, want 1 and 240", count, sum)
	}
}

// TestAFeedWithTimesButNoDelayObservesNothing checks that a feed which
// publishes absolute times and no delay produces no histogram at all.
//
// Rennes and MBTA both do this. An empty histogram would claim a punctuality
// that no producer ever stated.
func TestAFeedWithTimesButNoDelayObservesNothing(t *testing.T) {
	at := time.Now().Add(5 * time.Minute).Unix()
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("t", &gtfs.TripUpdate{
			Trip: &gtfs.TripDescriptor{TripId: proto.String("t1")},
			StopTimeUpdate: []*gtfs.TripUpdate_StopTimeUpdate{
				stopTime(1, nil, &at, nil, gtfs.TripUpdate_StopTimeUpdate_SCHEDULED),
			},
		}),
	}})
	subject := newCollector(t, server.URL, collector.Feed{})

	if count := testutil.CollectAndCount(subject, "gtfs_rt_trip_delay_seconds"); count != 0 {
		t.Errorf("got %d delay series, want 0", count)
	}
	if count := testutil.CollectAndCount(subject, "gtfs_rt_stop_time_updates"); count != 1 {
		t.Errorf("got %d stop time series, want 1", count)
	}
}

// TestAnAlertWithNoPeriodIsActive checks the rule of the specification. An
// alert with no period shows "as long as it appears in the feed".
func TestAnAlertWithNoPeriodIsActive(t *testing.T) {
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("a", &gtfs.Alert{
			Cause:         gtfs.Alert_STRIKE.Enum(),
			Effect:        gtfs.Alert_REDUCED_SERVICE.Enum(),
			SeverityLevel: gtfs.Alert_WARNING.Enum(),
		}),
	}})
	subject := newCollector(t, server.URL, collector.Feed{})

	expected := `
# HELP gtfs_rt_alerts Alerts by cause, effect, severity, and whether the alert is in force now.
# TYPE gtfs_rt_alerts gauge
gtfs_rt_alerts{active="true",cause="STRIKE",effect="REDUCED_SERVICE",severity_level="WARNING"} 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_alerts"); err != nil {
		t.Error(err)
	}
}

// TestImpactPeriodWinsOverActivePeriod checks that the exporter reads the
// current field first.
//
// The specification deprecated active_period and added communication_period
// and impact_period. A producer that sends both must be read through the newer
// field, so a stale active_period cannot mark a live alert as over.
func TestImpactPeriodWinsOverActivePeriod(t *testing.T) {
	now := uint64(time.Now().Unix())
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("a", &gtfs.Alert{
			// The deprecated field ended an hour ago.
			ActivePeriod: []*gtfs.TimeRange{{Start: proto.Uint64(now - 7200), End: proto.Uint64(now - 3600)}},
			// The current field says that the alert is in force.
			ImpactPeriod:  []*gtfs.TimeRange{{Start: proto.Uint64(now - 60), End: proto.Uint64(now + 3600)}},
			Cause:         gtfs.Alert_ACCIDENT.Enum(),
			Effect:        gtfs.Alert_DETOUR.Enum(),
			SeverityLevel: gtfs.Alert_SEVERE.Enum(),
		}),
	}})
	subject := newCollector(t, server.URL, collector.Feed{})

	expected := `
# HELP gtfs_rt_alerts Alerts by cause, effect, severity, and whether the alert is in force now.
# TYPE gtfs_rt_alerts gauge
gtfs_rt_alerts{active="true",cause="ACCIDENT",effect="DETOUR",severity_level="SEVERE"} 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_alerts"); err != nil {
		t.Error(err)
	}
}

// TestOneSelectorCountsEveryFieldThatItSets checks the counting rule of the
// informed entities.
//
// MBTA sets route_id, route_type and direction_id on one selector. The counts
// therefore do not sum to the number of selectors, and the help string warns
// against adding them.
func TestOneSelectorCountsEveryFieldThatItSets(t *testing.T) {
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("a", &gtfs.Alert{InformedEntity: []*gtfs.EntitySelector{{
			RouteId:     proto.String("Red"),
			RouteType:   proto.Int32(1),
			DirectionId: proto.Uint32(0),
		}}}),
	}})
	subject := newCollector(t, server.URL, collector.Feed{})

	expected := `
# HELP gtfs_rt_alert_informed_entities Selector fields of the informed entities of every alert, by field name. One selector can set several fields at once, so never add these series together.
# TYPE gtfs_rt_alert_informed_entities gauge
gtfs_rt_alert_informed_entities{selector="direction_id"} 1
gtfs_rt_alert_informed_entities{selector="route_id"} 1
gtfs_rt_alert_informed_entities{selector="route_type"} 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_alert_informed_entities"); err != nil {
		t.Error(err)
	}
}

// TestPerRouteIsOffByDefault checks the cardinality guard. Ile-de-France
// publishes 1973 routes, so the per-route family must never appear unless the
// module asks for it.
func TestPerRouteIsOffByDefault(t *testing.T) {
	message := &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("t", &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{
			TripId:  proto.String("t1"),
			RouteId: proto.String("r1"),
		}}),
	}}
	server := serveFeed(t, message)

	off := newCollector(t, server.URL, collector.Feed{})
	if count := testutil.CollectAndCount(off, "gtfs_rt_route_trip_updates"); count != 0 {
		t.Errorf("got %d per-route series with per_route off, want 0", count)
	}
	on := newCollector(t, server.URL, collector.Feed{PerRoute: true})
	if count := testutil.CollectAndCount(on, "gtfs_rt_route_trip_updates"); count != 1 {
		t.Errorf("got %d per-route series with per_route on, want 1", count)
	}
}

// TestDuplicateEntityIDsAreReported checks the conformance counter.
func TestDuplicateEntityIDsAreReported(t *testing.T) {
	vehicle := &gtfs.VehiclePosition{CurrentStatus: gtfs.VehiclePosition_STOPPED_AT.Enum()}
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("same", vehicle), entity("same", vehicle), entity("other", vehicle),
	}})
	subject := newCollector(t, server.URL, collector.Feed{})

	expected := `
# HELP gtfs_rt_duplicate_entity_ids Entity identifiers that the feed uses more than once.
# TYPE gtfs_rt_duplicate_entity_ids gauge
gtfs_rt_duplicate_entity_ids 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_duplicate_entity_ids"); err != nil {
		t.Error(err)
	}
}

// TestAnEntityWithNoPayloadIsCounted checks that a feed which sends an empty
// entity is visible.
func TestAnEntityWithNoPayloadIsCounted(t *testing.T) {
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		{Id: proto.String("empty")},
	}})
	subject := newCollector(t, server.URL, collector.Feed{})

	expected := `
# HELP gtfs_rt_entities_without_payload Entities that set none of the payload fields.
# TYPE gtfs_rt_entities_without_payload gauge
gtfs_rt_entities_without_payload 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_entities_without_payload"); err != nil {
		t.Error(err)
	}
}

// TestTheNativeHistogramCarriesNoClassicBuckets checks the trade that the
// README warns about.
//
// A constant native histogram is one form or the other. A scraper without
// native-histogram support sees only the sum and the count.
func TestTheNativeHistogramCarriesNoClassicBuckets(t *testing.T) {
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("a", &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{TripId: proto.String("t1")}, Delay: proto.Int32(-120)}),
		entity("b", &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{TripId: proto.String("t2")}, Delay: proto.Int32(0)}),
		entity("c", &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{TripId: proto.String("t3")}, Delay: proto.Int32(300)}),
	}})

	classic := metricOf(t, newCollector(t, server.URL, collector.Feed{}), "gtfs_rt_trip_delay_seconds")
	if len(classic.GetHistogram().GetBucket()) == 0 {
		t.Error("the classic form must carry buckets")
	}
	if classic.GetHistogram().Schema != nil {
		t.Error("the classic form must carry no native schema")
	}

	native := metricOf(t, newCollector(t, server.URL, collector.Feed{NativeHistogram: true}), "gtfs_rt_trip_delay_seconds")
	histogram := native.GetHistogram()
	if len(histogram.GetBucket()) != 0 {
		t.Errorf("the native form must carry no classic bucket, got %d", len(histogram.GetBucket()))
	}
	if histogram.GetSchema() == 0 && len(histogram.GetPositiveSpan()) == 0 && len(histogram.GetNegativeSpan()) == 0 {
		t.Error("the native form must carry exponential buckets")
	}
	// The zero bucket holds the exact zero, and the two others sit on each
	// side. The counts must add up, or the client library rejects the metric.
	if got := histogram.GetZeroCount(); got != 1 {
		t.Errorf("got zero count %d, want 1", got)
	}
	if got := histogram.GetSampleCount(); got != 3 {
		t.Errorf("got sample count %d, want 3", got)
	}
	if got := histogram.GetSampleSum(); got != 180 {
		t.Errorf("got sum %v, want 180", got)
	}
}

// gaugeOf returns the single gauge with the given name.
func gaugeOf(t *testing.T, subject prometheus.Collector, name string) prometheus.Gauge {
	t.Helper()
	metric := metricOf(t, subject, name)
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: name})
	gauge.Set(metric.GetGauge().GetValue())
	return gauge
}

// histogramOf returns the count and the sum of the named histogram.
func histogramOf(t *testing.T, subject prometheus.Collector, name string) (uint64, float64) {
	t.Helper()
	histogram := metricOf(t, subject, name).GetHistogram()
	return histogram.GetSampleCount(), histogram.GetSampleSum()
}

// metricOf collects the subject and returns the one metric with the name.
func metricOf(t *testing.T, subject prometheus.Collector, name string) *dto.Metric {
	t.Helper()
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(subject)
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("cannot gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() == name {
			if len(family.GetMetric()) != 1 {
				t.Fatalf("got %d series named %s, want 1", len(family.GetMetric()), name)
			}
			return family.GetMetric()[0]
		}
	}
	t.Fatalf("no metric named %s", name)
	return nil
}

// TestAVehicleWithNoTripDescriptorDoesNotPanic guards a crash that the first
// version of the collector had.
//
// GTFS-Realtime lets a vehicle report a position without a trip, for a vehicle
// that is not in service. The generated getter then returns nil, and a direct
// read of a field of that nil message ends the scrape. Every feed that was
// tested by hand happened to carry a trip, so only a fixture finds this.
func TestAVehicleWithNoTripDescriptorDoesNotPanic(t *testing.T) {
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("v", &gtfs.VehiclePosition{
			CurrentStatus: gtfs.VehiclePosition_IN_TRANSIT_TO.Enum(),
			Position:      &gtfs.Position{Latitude: proto.Float32(1), Longitude: proto.Float32(2)},
		}),
	}})
	subject := newCollector(t, server.URL, collector.Feed{})

	if count := testutil.CollectAndCount(subject, "gtfs_rt_vehicles"); count != 1 {
		t.Errorf("got %d vehicle series, want 1", count)
	}
	if count := testutil.CollectAndCount(subject, "gtfs_rt_entities_with_field"); count != 1 {
		t.Errorf("got %d coverage series, want 1 for the position alone", count)
	}
}

// TestAFeedThatOmitsARequiredFieldIsNotUp records how the decoder behaves.
//
// GTFS-Realtime is proto2, and it marks a few fields required, among them
// FeedEntity.id and TripUpdate.trip. A payload that leaves one out fails to
// decode as a whole, so the exporter reports the feed as down rather than
// reporting a feed with fewer entities. The test builds the bad payload by
// hand, because the marshaller refuses to write one.
func TestAFeedThatOmitsARequiredFieldIsNotUp(t *testing.T) {
	// A FeedMessage whose header omits gtfs_realtime_version. Field 1 of the
	// message is the header, and the header here is empty.
	body := []byte{0x0a, 0x00}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	subject := newCollector(t, server.URL, collector.Feed{})

	expected := `
# HELP gtfs_rt_up 1 if the exporter fetched and decoded the feed, 0 if it failed. Alert on this metric, and not on a metric that disappears.
# TYPE gtfs_rt_up gauge
gtfs_rt_up 0
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_up"); err != nil {
		t.Error(err)
	}
}

// staticLoader serves a small static feed and returns a loader whose index is
// ready.
func staticLoader(t *testing.T) *gtfsstatic.Loader {
	t.Helper()
	buffer := &bytes.Buffer{}
	writer := zip.NewWriter(buffer)
	for name, body := range map[string]string{
		"agency.txt": "agency_id,agency_name,agency_timezone\nSTAR,Star,Europe/Paris\n",
		"routes.txt": "route_id,route_short_name,route_type\nr1,a,1\n",
		"trips.txt":  "route_id,service_id,trip_id\nr1,s1,t1\n",
	} {
		file, err := writer.Create(name)
		if err != nil {
			t.Fatalf("cannot create %s: %v", name, err)
		}
		if _, err := file.Write([]byte(body)); err != nil {
			t.Fatalf("cannot write %s: %v", name, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("cannot close the zip: %v", err)
	}
	payload := buffer.Bytes()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
	}))
	t.Cleanup(server.Close)

	loader := gtfsstatic.NewLoader(
		gtfsstatic.Feed{URL: server.URL, RefreshInterval: time.Hour},
		server.Client(), slog.New(slog.DiscardHandler))
	loader.Start(t.Context())
	deadline := time.Now().Add(10 * time.Second)
	for loader.Index() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if loader.Index() == nil {
		t.Fatal("the static index never became ready")
	}
	return loader
}

// TestAFeedWithoutRouteIDsUsesTheStaticIndex is the reason that the static
// package exists.
//
// SNCF publishes 1140 trip updates and no route identifier on any of them.
// The route comes from the trip through the static feed instead.
func TestAFeedWithoutRouteIDsUsesTheStaticIndex(t *testing.T) {
	message := &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("e1", &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{TripId: proto.String("t1")}}),
	}}
	server := serveFeed(t, message)
	subject := newCollector(t, server.URL, collector.Feed{PerRoute: true, Static: staticLoader(t)})

	expected := `
# HELP gtfs_rt_route_trip_updates Trip updates per route. Only the route identifier is a label. Join gtfs_static_route_info for the name. The series appear only when the module sets per_route.
# TYPE gtfs_rt_route_trip_updates gauge
gtfs_rt_route_trip_updates{route_id="r1"} 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_route_trip_updates"); err != nil {
		t.Error(err)
	}

	// The coverage metric still reports what the producer published, and not
	// what the index resolved.
	if count := testutil.CollectAndCount(subject, "gtfs_rt_entities_with_field"); count != 1 {
		t.Errorf("got %d coverage series, want 1 for the trip identifier alone", count)
	}
	info := `
# HELP gtfs_static_route_info Metadata of a route that the realtime feed names. The value is always 1. Join it with gtfs_rt_route_trip_updates on route_id, so that the counting metric carries the identifier alone.
# TYPE gtfs_static_route_info gauge
gtfs_static_route_info{agency_id="STAR",route_id="r1",route_short_name="a",route_type="1"} 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(info), "gtfs_static_route_info"); err != nil {
		t.Error(err)
	}
}

// TestAFeedWithoutRouteIDsAndNoStaticIndexHasNoRouteSeries checks the honest
// answer when no static feed is configured. The exporter writes no route
// series at all, rather than a series labelled unknown.
func TestAFeedWithoutRouteIDsAndNoStaticIndexHasNoRouteSeries(t *testing.T) {
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("e1", &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{TripId: proto.String("t1")}}),
	}})
	subject := newCollector(t, server.URL, collector.Feed{PerRoute: true})

	if count := testutil.CollectAndCount(subject, "gtfs_rt_route_trip_updates"); count != 0 {
		t.Errorf("got %d route series with no static feed, want 0", count)
	}
	if count := testutil.CollectAndCount(subject, "gtfs_static_loaded"); count != 0 {
		t.Errorf("got %d static series with no static feed, want 0", count)
	}
	// The trip is still counted, so nothing disappears.
	if count := testutil.CollectAndCount(subject, "gtfs_rt_trip_updates"); count != 1 {
		t.Errorf("got %d trip series, want 1", count)
	}
}

// TestAnAddedTripIsNotCountedAsMissingFromStatic guards the mismatch metric
// against a permanent false positive.
//
// A trip marked ADDED, NEW or DUPLICATED has no static row by design, and a
// trip named without an identifier cannot be looked up at all.
func TestAnAddedTripIsNotCountedAsMissingFromStatic(t *testing.T) {
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("known", &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{TripId: proto.String("t1")}}),
		entity("added", &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{
			TripId:               proto.String("extra-1"),
			ScheduleRelationship: gtfs.TripDescriptor_ADDED.Enum(),
		}}),
		entity("new", &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{
			TripId:               proto.String("extra-2"),
			ScheduleRelationship: gtfs.TripDescriptor_NEW.Enum(),
		}}),
		entity("duplicated", &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{
			TripId:               proto.String("extra-3"),
			ScheduleRelationship: gtfs.TripDescriptor_DUPLICATED.Enum(),
		}}),
		entity("nameless", &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{
			RouteId:   proto.String("r1"),
			StartTime: proto.String("10:00:00"),
		}}),
		entity("stale", &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{TripId: proto.String("gone")}}),
	}})
	subject := newCollector(t, server.URL, collector.Feed{Static: staticLoader(t)})

	// Only the scheduled trip that the dataset does not hold counts.
	expected := `
# HELP gtfs_rt_trips_not_in_static Trips of the realtime feed that the static feed does not hold. Added, new and duplicated trips are left out, because they have no static row by design. A high value usually means that the two feeds are of different datasets, so read gtfs_static_info first.
# TYPE gtfs_rt_trips_not_in_static gauge
gtfs_rt_trips_not_in_static 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_trips_not_in_static"); err != nil {
		t.Error(err)
	}
}

// TestAScrapeNeverWaitsForTheStaticLoad checks the invariant that keeps the
// exporter usable at start.
//
// Reading the Ile-de-France dataset takes about fifteen seconds. A scrape that
// waited for it would time out, and Prometheus would record nothing at all.
func TestAScrapeNeverWaitsForTheStaticLoad(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(slow.Close)

	loader := gtfsstatic.NewLoader(
		gtfsstatic.Feed{URL: slow.URL, RefreshInterval: time.Hour},
		slow.Client(), slog.New(slog.DiscardHandler))
	loader.Start(t.Context())

	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("e1", &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{TripId: proto.String("t1")}}),
	}})
	subject := newCollector(t, server.URL, collector.Feed{PerRoute: true, Static: loader})

	done := make(chan struct{})
	go func() {
		defer close(done)
		// The realtime metrics must all be there while the static load hangs.
		if count := testutil.CollectAndCount(subject, "gtfs_rt_trip_updates"); count != 1 {
			t.Errorf("got %d trip series, want 1", count)
		}
		expected := `
# HELP gtfs_static_loaded 1 when a static index is ready, 0 while the first load runs. A scrape never waits for a load.
# TYPE gtfs_static_loaded gauge
gtfs_static_loaded 0
`
		if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_static_loaded"); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the scrape waited for the static load")
	}
}

// TestRouteInfoIsGatedOnPerRoute checks that the reference series follow the
// counting series that they exist to be joined with.
//
// Before this gate, a module that left per_route off still published one
// gtfs_static_route_info series per route, with nothing to join to. SNCF
// produced 460 of them.
func TestRouteInfoIsGatedOnPerRoute(t *testing.T) {
	message := &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("e1", &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{TripId: proto.String("t1")}}),
	}}
	server := serveFeed(t, message)
	loader := staticLoader(t)

	off := newCollector(t, server.URL, collector.Feed{Static: loader})
	if count := testutil.CollectAndCount(off, "gtfs_static_route_info"); count != 0 {
		t.Errorf("got %d route info series with per_route off, want 0", count)
	}
	// The rest of the static family must still appear, because it reports the
	// state of the index and not the routes of the feed.
	if count := testutil.CollectAndCount(off, "gtfs_static_loaded"); count != 1 {
		t.Errorf("got %d gtfs_static_loaded series, want 1", count)
	}

	on := newCollector(t, server.URL, collector.Feed{PerRoute: true, Static: loader})
	if count := testutil.CollectAndCount(on, "gtfs_static_route_info"); count != 1 {
		t.Errorf("got %d route info series with per_route on, want 1", count)
	}
}
