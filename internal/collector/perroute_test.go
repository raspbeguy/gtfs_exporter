package collector_test

import (
	"strings"
	"testing"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"

	"github.com/raspbeguy/gtfs_exporter/internal/collector"
)

// twoRoutes builds a feed of trips on r1 and r2 with stated delays, so the
// per-route tallies can be checked without a timetable.
func twoRoutes() *gtfs.FeedMessage {
	trip := func(id, route string, delay int32, rel *gtfs.TripDescriptor_ScheduleRelationship) *gtfs.FeedEntity {
		d := &gtfs.TripDescriptor{TripId: proto.String(id), RouteId: proto.String(route)}
		d.ScheduleRelationship = rel
		return entity(id, &gtfs.TripUpdate{Trip: d, Delay: proto.Int32(delay)})
	}
	cancelled := gtfs.TripDescriptor_CANCELED.Enum()
	return &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		trip("a", "r1", 30, nil),  // on time
		trip("b", "r1", 600, nil), // late
		trip("c", "r1", 900, nil), // late
		trip("d", "r2", 60, nil),  // on time
		trip("e", "r2", 0, cancelled),
	}}
}

// TestPerRouteIsOffByDefaultForEveryFamily checks the cardinality guard. The
// per-route families cost about twenty series a route, so a module must ask.
func TestPerRouteIsOffByDefaultForEveryFamily(t *testing.T) {
	server := serveFeed(t, twoRoutes())
	subject := newCollector(t, server.URL, collector.Feed{})

	for _, name := range []string{
		"gtfs_rt_route_trip_updates", "gtfs_rt_route_trip_delay_seconds",
		"gtfs_rt_route_vehicles", "gtfs_rt_route_trips_cancelled",
		"gtfs_rt_route_trips_late", "gtfs_rt_route_alerts",
	} {
		if count := testutil.CollectAndCount(subject, name); count != 0 {
			t.Errorf("%s: got %d series with per_route off, want 0", name, count)
		}
	}
	// The aggregate is unaffected.
	if count := testutil.CollectAndCount(subject, "gtfs_rt_trip_delay_seconds"); count != 1 {
		t.Errorf("got %d aggregate histograms, want 1", count)
	}
}

// TestPerRouteDelayIsItsOwnFlag checks that the expensive family is separate
// from the cheap one. The counts are about three series a route, the histogram
// is eighteen.
func TestPerRouteDelayIsItsOwnFlag(t *testing.T) {
	server := serveFeed(t, twoRoutes())

	cheap := newCollector(t, server.URL, collector.Feed{PerRoute: true})
	if count := testutil.CollectAndCount(cheap, "gtfs_rt_route_trip_updates"); count != 2 {
		t.Errorf("got %d route count series, want 2", count)
	}
	if count := testutil.CollectAndCount(cheap, "gtfs_rt_route_trip_delay_seconds"); count != 0 {
		t.Errorf("got %d route histograms without per_route_delay, want 0", count)
	}

	full := newCollector(t, server.URL, collector.Feed{PerRoute: true, PerRouteDelay: true})
	if count := testutil.CollectAndCount(full, "gtfs_rt_route_trip_delay_seconds"); count != 2 {
		t.Errorf("got %d route histograms, want 2", count)
	}
}

// TestTheRouteHistogramSumsToTheAggregate is the invariant that justifies two
// metric names instead of one optional label. A reader must be able to trust
// that the split does not change the total.
func TestTheRouteHistogramSumsToTheAggregate(t *testing.T) {
	server := serveFeed(t, twoRoutes())
	subject := newCollector(t, server.URL, collector.Feed{PerRoute: true, PerRouteDelay: true})

	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(subject)
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("cannot gather: %v", err)
	}
	var aggCount, routeCount uint64
	var aggSum, routeSum float64
	for _, f := range families {
		for _, m := range f.GetMetric() {
			h := m.GetHistogram()
			switch f.GetName() {
			case "gtfs_rt_trip_delay_seconds":
				aggCount += h.GetSampleCount()
				aggSum += h.GetSampleSum()
			case "gtfs_rt_route_trip_delay_seconds":
				routeCount += h.GetSampleCount()
				routeSum += h.GetSampleSum()
			}
		}
	}
	if aggCount == 0 {
		t.Fatal("the aggregate histogram is missing")
	}
	if aggCount != routeCount || aggSum != routeSum {
		t.Errorf("aggregate has count %d sum %v, the per-route split has %d and %v",
			aggCount, aggSum, routeCount, routeSum)
	}
}

// TestALateThresholdCountsOnlyTripsPastIt checks the number that a status
// board puts in its largest type.
func TestALateThresholdCountsOnlyTripsPastIt(t *testing.T) {
	server := serveFeed(t, twoRoutes())
	subject := newCollector(t, server.URL,
		collector.Feed{PerRoute: true, LateThreshold: 300 * time.Second})

	// r1 has one trip at 30 s and two past 300 s. r2 has one at 60 s and one
	// cancelled, which never carries a delay.
	expected := `
# HELP gtfs_rt_route_trips_late Trips on the route running later than the threshold, in seconds. The histogram carries the same information, but this one survives when per_route_delay is off.
# TYPE gtfs_rt_route_trips_late gauge
gtfs_rt_route_trips_late{route_id="r1",threshold="300"} 2
gtfs_rt_route_trips_late{route_id="r2",threshold="300"} 0
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_route_trips_late"); err != nil {
		t.Error(err)
	}
}

// TestARouteWithNoCancellationHasNoSeries keeps the sparse families sparse.
// A route that is running normally must not carry a zero for every fault it
// does not have.
func TestARouteWithNoCancellationHasNoSeries(t *testing.T) {
	server := serveFeed(t, twoRoutes())
	subject := newCollector(t, server.URL, collector.Feed{PerRoute: true})

	expected := `
# HELP gtfs_rt_route_trips_cancelled Cancelled trips on the route. A route with none gets no series.
# TYPE gtfs_rt_route_trips_cancelled gauge
gtfs_rt_route_trips_cancelled{route_id="r2"} 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_route_trips_cancelled"); err != nil {
		t.Error(err)
	}
}

// TestARouteWithNoVehiclesHasNoVehicleSeries checks the same rule for the
// vehicle family. SNCF publishes no vehicle feed at all.
func TestARouteWithNoVehiclesHasNoVehicleSeries(t *testing.T) {
	server := serveFeed(t, twoRoutes()) // trip updates only, no positions
	subject := newCollector(t, server.URL, collector.Feed{PerRoute: true})

	if count := testutil.CollectAndCount(subject, "gtfs_rt_route_vehicles"); count != 0 {
		t.Errorf("got %d vehicle series from a feed with no positions, want 0", count)
	}
}

func TestVehiclesAreCountedAgainstTheirRoute(t *testing.T) {
	position := func(id, route string) *gtfs.FeedEntity {
		return entity(id, &gtfs.VehiclePosition{
			Trip:          &gtfs.TripDescriptor{RouteId: proto.String(route)},
			CurrentStatus: gtfs.VehiclePosition_IN_TRANSIT_TO.Enum(),
		})
	}
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		position("v1", "r1"), position("v2", "r1"), position("v3", "r2"),
	}})
	subject := newCollector(t, server.URL, collector.Feed{PerRoute: true})

	expected := `
# HELP gtfs_rt_route_vehicles Vehicles reporting a position on the route. A route with no vehicle gets no series, and a feed with no vehicle positions gets none at all.
# TYPE gtfs_rt_route_vehicles gauge
gtfs_rt_route_vehicles{route_id="r1"} 2
gtfs_rt_route_vehicles{route_id="r2"} 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_route_vehicles"); err != nil {
		t.Error(err)
	}
}

var _ = dto.Metric{}

// alertWith builds a feed holding one active alert with the given selectors.
func alertWith(effect gtfs.Alert_Effect, selectors ...*gtfs.EntitySelector) *gtfs.FeedMessage {
	return &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("a1", &gtfs.Alert{
			Effect:         effect.Enum(),
			Cause:          gtfs.Alert_ACCIDENT.Enum(),
			SeverityLevel:  gtfs.Alert_WARNING.Enum(),
			InformedEntity: selectors,
		}),
	}}
}

// TestAnAlertIsCountedOncePerRouteNotPerSelector is the trap that makes this
// family affordable.
//
// SNCF publishes 314 alerts carrying 14,464 trip selectors. Counting per
// selector would report the number of affected routes wrong by a factor of
// forty, and would put more series on this metric than the whole per-route
// histogram.
func TestAnAlertIsCountedOncePerRouteNotPerSelector(t *testing.T) {
	// Twelve selectors, all naming the same route.
	var many []*gtfs.EntitySelector
	for range 12 {
		many = append(many, &gtfs.EntitySelector{RouteId: proto.String("r1")})
	}
	server := serveFeed(t, alertWith(gtfs.Alert_SIGNIFICANT_DELAYS, many...))
	subject := newCollector(t, server.URL, collector.Feed{PerRoute: true})

	expected := `
# HELP gtfs_rt_route_alerts Alerts in force that name this route, counted once per route and effect rather than once per selector. SNCF names a trip on 14,464 selectors across 314 alerts, so counting selectors would report the wrong number by a factor of forty.
# TYPE gtfs_rt_route_alerts gauge
gtfs_rt_route_alerts{effect="SIGNIFICANT_DELAYS",route_id="r1"} 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_route_alerts"); err != nil {
		t.Error(err)
	}
}

// TestAnAlertNamingATripResolvesToItsRoute checks the path that SNCF forces.
// It names a trip on 14,464 of its selectors and a route on none.
func TestAnAlertNamingATripResolvesToItsRoute(t *testing.T) {
	loader := staticLoader(t) // trip t1 runs route r1
	server := serveFeed(t, alertWith(gtfs.Alert_DETOUR,
		&gtfs.EntitySelector{Trip: &gtfs.TripDescriptor{TripId: proto.String("t1")}}))
	subject := newCollector(t, server.URL, collector.Feed{PerRoute: true, Static: loader})

	expected := `
# HELP gtfs_rt_route_alerts Alerts in force that name this route, counted once per route and effect rather than once per selector. SNCF names a trip on 14,464 selectors across 314 alerts, so counting selectors would report the wrong number by a factor of forty.
# TYPE gtfs_rt_route_alerts gauge
gtfs_rt_route_alerts{effect="DETOUR",route_id="r1"} 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_route_alerts"); err != nil {
		t.Error(err)
	}
}

// TestAnAgencyWideAlertIsCountedAsUnresolved keeps a network-wide disruption
// visible. Without this series a status board would show every line green
// during a strike.
func TestAnAgencyWideAlertIsCountedAsUnresolved(t *testing.T) {
	server := serveFeed(t, alertWith(gtfs.Alert_NO_SERVICE,
		&gtfs.EntitySelector{AgencyId: proto.String("STAR")}))
	subject := newCollector(t, server.URL, collector.Feed{PerRoute: true})

	if count := testutil.CollectAndCount(subject, "gtfs_rt_route_alerts"); count != 0 {
		t.Errorf("got %d route alerts for an agency-wide one, want 0", count)
	}
	expected := `
# HELP gtfs_rt_alerts_unresolved Alerts that name no route, by the widest scope they do name. An agency or route_type alert affects the whole network, and without this series a status board would show nothing at all during one.
# TYPE gtfs_rt_alerts_unresolved gauge
gtfs_rt_alerts_unresolved{scope="agency"} 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_alerts_unresolved"); err != nil {
		t.Error(err)
	}
}

// TestAnAlertThatIsNotInForceIsNotOnTheBoard checks that a board shows a
// disruption only while it is happening.
func TestAnAlertThatIsNotInForceIsNotOnTheBoard(t *testing.T) {
	past := uint64(time.Now().Add(-48 * time.Hour).Unix())
	ended := uint64(time.Now().Add(-24 * time.Hour).Unix())
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		entity("a1", &gtfs.Alert{
			ActivePeriod:   []*gtfs.TimeRange{{Start: &past, End: &ended}},
			Effect:         gtfs.Alert_DETOUR.Enum(),
			InformedEntity: []*gtfs.EntitySelector{{RouteId: proto.String("r1")}},
		}),
	}})
	subject := newCollector(t, server.URL, collector.Feed{PerRoute: true})

	if count := testutil.CollectAndCount(subject, "gtfs_rt_route_alerts"); count != 0 {
		t.Errorf("got %d route alerts for an alert that ended yesterday, want 0", count)
	}
	if count := testutil.CollectAndCount(subject, "gtfs_rt_alerts_unresolved"); count != 0 {
		t.Errorf("got %d unresolved for an alert that ended yesterday, want 0", count)
	}
}

// TestAnAlertNamingAnUnknownTripIsNamedAsSuch separates a data fault from an
// absence.
//
// SNCF writes a short trip identifier in its alerts, such as OCEEA436112R, and
// the long composite one in trips.txt. None of its 7,231 alert trips resolves,
// and an operator needs to see that rather than read it as "this alert names
// nothing".
func TestAnAlertNamingAnUnknownTripIsNamedAsSuch(t *testing.T) {
	loader := staticLoader(t) // holds trip t1 only
	server := serveFeed(t, alertWith(gtfs.Alert_REDUCED_SERVICE,
		&gtfs.EntitySelector{Trip: &gtfs.TripDescriptor{TripId: proto.String("not-in-the-timetable")}}))
	subject := newCollector(t, server.URL, collector.Feed{PerRoute: true, Static: loader})

	expected := `
# HELP gtfs_rt_alerts_unresolved Alerts that name no route, by the widest scope they do name. An agency or route_type alert affects the whole network, and without this series a status board would show nothing at all during one.
# TYPE gtfs_rt_alerts_unresolved gauge
gtfs_rt_alerts_unresolved{scope="unresolved_trip"} 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_alerts_unresolved"); err != nil {
		t.Error(err)
	}
}

// metricOf2 returns the metric of a family whose first label value matches.
func metricOf2(t *testing.T, subject prometheus.Collector, name, label string) *dto.Metric {
	t.Helper()
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(subject)
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("cannot gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetValue() == label {
					return m
				}
			}
		}
	}
	t.Fatalf("no %s with label %s", name, label)
	return nil
}

// TestPerRouteDelayWorksAsANativeHistogram covers the combination that no
// other test reaches. A constant native histogram takes its label values last,
// and getting that wrong panics inside the scrape rather than failing to
// build.
func TestPerRouteDelayWorksAsANativeHistogram(t *testing.T) {
	server := serveFeed(t, twoRoutes())
	subject := newCollector(t, server.URL, collector.Feed{
		PerRoute: true, PerRouteDelay: true, NativeHistogram: true,
	})
	if count := testutil.CollectAndCount(subject, "gtfs_rt_route_trip_delay_seconds"); count != 2 {
		t.Errorf("got %d native route histograms, want 2", count)
	}
	metric := metricOf2(t, subject, "gtfs_rt_route_trip_delay_seconds", "r1")
	if len(metric.GetHistogram().GetBucket()) != 0 {
		t.Error("the native form must carry no classic bucket")
	}
	if metric.GetHistogram().GetSampleCount() != 3 {
		t.Errorf("got count %d, want the 3 trips of r1", metric.GetHistogram().GetSampleCount())
	}
}

// TestADeletedTripCountsAsWithdrawn keeps a trip from vanishing.
//
// tripDelay excludes CANCELED and DELETED alike, so counting only CANCELED as
// withdrawn would let a deleted trip disappear: absent from the punctuality
// and absent from the cancellation count, so a board would show the line as
// running normally.
func TestADeletedTripCountsAsWithdrawn(t *testing.T) {
	withRelation := func(id, route string, rel gtfs.TripDescriptor_ScheduleRelationship) *gtfs.FeedEntity {
		return entity(id, &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{
			TripId: proto.String(id), RouteId: proto.String(route),
			ScheduleRelationship: rel.Enum(),
		}, Delay: proto.Int32(0)})
	}
	server := serveFeed(t, &gtfs.FeedMessage{Header: header(), Entity: []*gtfs.FeedEntity{
		withRelation("a", "r1", gtfs.TripDescriptor_CANCELED),
		withRelation("b", "r1", gtfs.TripDescriptor_DELETED),
		withRelation("c", "r2", gtfs.TripDescriptor_SCHEDULED),
	}})
	subject := newCollector(t, server.URL, collector.Feed{PerRoute: true})

	expected := `
# HELP gtfs_rt_route_trips_cancelled Cancelled trips on the route. A route with none gets no series.
# TYPE gtfs_rt_route_trips_cancelled gauge
gtfs_rt_route_trips_cancelled{route_id="r1"} 2
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_route_trips_cancelled"); err != nil {
		t.Error(err)
	}
}
