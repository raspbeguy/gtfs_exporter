// Package collector turns GTFS-Realtime feeds into Prometheus metrics.
package collector

import (
	"context"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/raspbeguy/gtfs_exporter/internal/gtfsrt"
	"github.com/raspbeguy/gtfs_exporter/internal/gtfsstatic"
)

const namespace = "gtfs_rt"

// defaultDelayBuckets covers the delays that real feeds publish. The feeds
// that this exporter was tested against run from -493 seconds early to 7800
// seconds late, and their median delay is exactly 0.
var defaultDelayBuckets = []float64{
	-600, -300, -120, -60, -30, 0, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200,
}

// nativeSchema and nativeZeroThreshold configure the optional native
// histogram. A threshold of 0.5 keeps an exact delay of 0 in the zero bucket
// and every whole second outside it, because GTFS-Realtime states a delay in
// whole seconds.
const (
	nativeSchema        = 3
	nativeZeroThreshold = 0.5
)

var (
	upDesc = prometheus.NewDesc(
		namespace+"_up",
		"1 if the exporter fetched and decoded the feed, 0 if it failed. Alert on this metric, and not on a metric that disappears.",
		nil, nil)

	feedTimestampDesc = prometheus.NewDesc(
		namespace+"_feed_timestamp_seconds",
		"Unix time of the feed header, exactly as the producer wrote it. Some producers write a time in the future, so the value is never clamped. Staleness is time() minus this value.",
		nil, nil)

	fetchDurationDesc = prometheus.NewDesc(
		namespace+"_fetch_duration_seconds",
		"Seconds that the exporter took to fetch and decode the feed.",
		nil, nil)

	feedSizeDesc = prometheus.NewDesc(
		namespace+"_feed_size_bytes",
		"Size of the payload that the producer sent.",
		nil, nil)

	feedInfoDesc = prometheus.NewDesc(
		namespace+"_feed_info",
		"Feed header metadata. The value is always 1.",
		[]string{"gtfs_realtime_version", "incrementality"}, nil)

	entitiesDesc = prometheus.NewDesc(
		namespace+"_entities",
		"Entities in the feed, by kind. A kind that the feed does not carry gets no series.",
		[]string{"entity_type"}, nil)

	tripUpdatesDesc = prometheus.NewDesc(
		namespace+"_trip_updates",
		"Trip updates by schedule relationship. The CANCELED series is the number of cancelled trips.",
		[]string{"schedule_relationship"}, nil)

	stopTimeUpdatesDesc = prometheus.NewDesc(
		namespace+"_stop_time_updates",
		"Stop time updates by schedule relationship. The SKIPPED series is the number of skipped stops.",
		[]string{"schedule_relationship"}, nil)

	routeTripUpdatesDesc = prometheus.NewDesc(
		namespace+"_route_trip_updates",
		"Trip updates per route. Only the route identifier is a label. Join gtfs_static_route_info for the name. The series appear only when the module sets per_route.",
		[]string{"route_id"}, nil)

	tripDelayDesc = prometheus.NewDesc(
		namespace+"_trip_delay_seconds",
		"Delay of each trip in seconds, negative when the trip is early. One observation per trip. A cancelled trip and a stop with no prediction are left out, and a feed that states no delay gets no series at all.",
		nil, nil)

	vehiclesDesc = prometheus.NewDesc(
		namespace+"_vehicles",
		"Vehicle positions by stop status. Sum this metric for the number of vehicles.",
		[]string{"current_status"}, nil)

	vehicleOccupancyDesc = prometheus.NewDesc(
		namespace+"_vehicle_occupancy",
		"Vehicles by occupancy status. Only 2 of 18 tested feeds publish this field, and a feed that omits it gets no series. Never add this metric to gtfs_rt_vehicles.",
		[]string{"occupancy_status"}, nil)

	vehicleCongestionDesc = prometheus.NewDesc(
		namespace+"_vehicle_congestion",
		"Vehicles by congestion level. A feed that omits the field gets no series. Never add this metric to gtfs_rt_vehicles.",
		[]string{"congestion_level"}, nil)

	vehicleAgeDesc = prometheus.NewDesc(
		namespace+"_vehicle_timestamp_age_seconds",
		"Age of each vehicle timestamp in seconds, measured against the time of the read. A vehicle that states no timestamp is left out.",
		nil, nil)

	alertsDesc = prometheus.NewDesc(
		namespace+"_alerts",
		"Alerts by cause, effect, severity, and whether the alert is in force now.",
		[]string{"cause", "effect", "severity_level", "active"}, nil)

	alertSelectorsDesc = prometheus.NewDesc(
		namespace+"_alert_informed_entities",
		"Selector fields of the informed entities of every alert, by field name. One selector can set several fields at once, so never add these series together.",
		[]string{"selector"}, nil)

	entitiesWithFieldDesc = prometheus.NewDesc(
		namespace+"_entities_with_field",
		"Entities that carry an optional field. A count and not a ratio, because coverage inside one feed is partial. Divide by gtfs_rt_entities for a ratio.",
		[]string{"entity_type", "field"}, nil)

	duplicateEntityIDsDesc = prometheus.NewDesc(
		namespace+"_duplicate_entity_ids",
		"Entity identifiers that the feed uses more than once.",
		nil, nil)

	entitiesWithoutPayloadDesc = prometheus.NewDesc(
		namespace+"_entities_without_payload",
		"Entities that set none of the payload fields.",
		nil, nil)

	entitiesDeletedDesc = prometheus.NewDesc(
		namespace+"_entities_deleted",
		"Entities that set is_deleted on a FULL_DATASET feed, which the specification forbids.",
		nil, nil)

	tripsNotInStaticDesc = prometheus.NewDesc(
		namespace+"_trips_not_in_static",
		"Trips of the realtime feed that the static feed does not hold. Added, new and duplicated trips are left out, because they have no static row by design. A high value usually means that the two feeds are of different datasets, so read gtfs_static_info first.",
		nil, nil)

	staticLoadedDesc = prometheus.NewDesc(
		"gtfs_static_loaded",
		"1 when a static index is ready, 0 while the first load runs. A scrape never waits for a load.",
		nil, nil)

	staticInfoDesc = prometheus.NewDesc(
		"gtfs_static_info",
		"Metadata of the loaded static dataset, from feed_info.txt. The value is always 1.",
		[]string{"feed_version", "feed_start_date", "feed_end_date"}, nil)

	staticLoadTimestampDesc = prometheus.NewDesc(
		"gtfs_static_last_load_timestamp_seconds",
		"Unix time when the current static index was built.",
		nil, nil)

	staticLoadDurationDesc = prometheus.NewDesc(
		"gtfs_static_load_duration_seconds",
		"Seconds that the last successful static load took.",
		nil, nil)

	staticBytesDesc = prometheus.NewDesc(
		"gtfs_static_bytes_fetched",
		"Bytes of the static zip that the last load read. Compare it with the size of the file to see what the range requests saved.",
		nil, nil)

	staticAgenciesDesc = prometheus.NewDesc(
		"gtfs_static_agencies", "Agencies in the static index.", nil, nil)
	staticRoutesDesc = prometheus.NewDesc(
		"gtfs_static_routes", "Routes in the static index.", nil, nil)
	staticTripsDesc = prometheus.NewDesc(
		"gtfs_static_trips", "Trips in the static index.", nil, nil)

	routeDelayDesc = prometheus.NewDesc(
		namespace+"_route_trip_delay_seconds",
		"Delay of each trip in seconds, per route. A separate metric from gtfs_rt_trip_delay_seconds rather than a label on it, so that the aggregate keeps its shape: a sum over this one equals it. Eighteen series a route, so the module must set per_route_delay.",
		[]string{"route_id"}, nil)

	routeVehiclesDesc = prometheus.NewDesc(
		namespace+"_route_vehicles",
		"Vehicles reporting a position on the route. A route with no vehicle gets no series, and a feed with no vehicle positions gets none at all.",
		[]string{"route_id"}, nil)

	routeAlertsDesc = prometheus.NewDesc(
		namespace+"_route_alerts",
		"Alerts in force that name this route, counted once per route and effect rather than once per selector. SNCF names a trip on 14,464 selectors across 314 alerts, so counting selectors would report the wrong number by a factor of forty.",
		[]string{"route_id", "effect"}, nil)

	routeCancelledDesc = prometheus.NewDesc(
		namespace+"_route_trips_cancelled",
		"Cancelled trips on the route. A route with none gets no series.",
		[]string{"route_id"}, nil)

	routeLateDesc = prometheus.NewDesc(
		namespace+"_route_trips_late",
		"Trips on the route running later than the threshold, in seconds. The histogram carries the same information, but this one survives when per_route_delay is off.",
		[]string{"route_id", "threshold"}, nil)

	alertsUnresolvedDesc = prometheus.NewDesc(
		namespace+"_alerts_unresolved",
		"Alerts that name no route, by the widest scope they do name. An agency or route_type alert affects the whole network, and without this series a status board would show nothing at all during one.",
		[]string{"scope"}, nil)

	delaySourceDesc = prometheus.NewDesc(
		namespace+"_delay_source_info",
		"Where the delay of this feed comes from. The value is always 1. A producer value is what the operator published. A computed value is what this exporter worked out from the static timetable, which is a weaker reading.",
		[]string{"source"}, nil)

	tripsWithoutScheduleDesc = prometheus.NewDesc(
		namespace+"_trips_without_schedule",
		"Live trips that the timetable cannot time, by reason. Several reasons are healthy: no_timeable_stop is an update that skips a stop or two on a trip which is still running, and MBTA sends about 40% of its trip updates that way. Never alert on the total without splitting the label.",
		[]string{"reason"}, nil)

	staticScheduleRowsDesc = prometheus.NewDesc(
		"gtfs_static_schedule_rows",
		"Stop times held in the timetable. No series at all when the module did not ask for a schedule, and 0 when it did and the timetable is not ready or was refused.",
		nil, nil)

	staticInServiceDesc = prometheus.NewDesc(
		"gtfs_static_in_service",
		"1 when the service day falls inside the validity window of the dataset. The exporter computes no delay while this is 0, because a timetable that has expired gives a wrong answer rather than no answer.",
		nil, nil)

	staticExpiresDesc = prometheus.NewDesc(
		"gtfs_static_expires_timestamp_seconds",
		"Unix time when the loaded dataset stops describing the service. No series when the feed states no date. A dataset lives one to two weeks, so alert well before this.",
		nil, nil)

	staticPendingDesc = prometheus.NewDesc(
		"gtfs_static_pending",
		"1 while a newer dataset is held back because its start date has not arrived. A producer publishes the next timetable before it takes effect, and installing it early would answer with a service that does not run.",
		nil, nil)

	staticRouteInfoDesc = prometheus.NewDesc(
		"gtfs_static_route_info",
		"Metadata of a route that the realtime feed names. The value is always 1. Join it with gtfs_rt_route_trip_updates on route_id, so that the counting metric carries the identifier alone.",
		[]string{"route_id", "route_short_name", "route_type", "agency_id"}, nil)

	allDescs = []*prometheus.Desc{
		upDesc, feedTimestampDesc, fetchDurationDesc, feedSizeDesc, feedInfoDesc,
		entitiesDesc, tripUpdatesDesc, stopTimeUpdatesDesc, routeTripUpdatesDesc,
		tripDelayDesc, vehiclesDesc, vehicleOccupancyDesc, vehicleCongestionDesc,
		vehicleAgeDesc, alertsDesc, alertSelectorsDesc, entitiesWithFieldDesc,
		duplicateEntityIDsDesc, entitiesWithoutPayloadDesc, entitiesDeletedDesc,
		tripsNotInStaticDesc, staticLoadedDesc, staticInfoDesc,
		staticLoadTimestampDesc, staticLoadDurationDesc, staticBytesDesc,
		staticAgenciesDesc, staticRoutesDesc, staticTripsDesc, staticRouteInfoDesc,
		delaySourceDesc, tripsWithoutScheduleDesc, staticScheduleRowsDesc,
		staticInServiceDesc, staticExpiresDesc, staticPendingDesc,
		routeDelayDesc, routeVehiclesDesc, routeAlertsDesc, routeCancelledDesc,
		routeLateDesc, alertsUnresolvedDesc,
	}
)

// Feed is the one feed that a scrape reads.
type Feed struct {
	// Name reaches the log only. Prometheus owns the target identity.
	Name            string
	URL             string
	PerRoute        bool
	Headers         map[string]string
	RequestTimeout  time.Duration
	NoKeepAlive     bool
	DelayBuckets    []float64
	NativeHistogram bool
	// Schedule mirrors the module setting. It decides whether
	// gtfs_static_schedule_rows appears at all.
	Schedule bool
	// PerRouteDelay adds the delay histogram per route, which is eighteen
	// series a route. It needs PerRoute, because the histogram carries only
	// the identifier and the name comes from gtfs_static_route_info.
	PerRouteDelay bool
	// LateThreshold is the delay past which a trip counts as late on a status
	// board. A metro and a regional train do not agree on what late means.
	LateThreshold time.Duration
	// Static is the loader of the static feed of this network, or nil when the
	// module names none. Several realtime feeds of one network share one
	// loader.
	Static *gtfsstatic.Loader
}

// Collector reads one feed when Prometheus scrapes it.
type Collector struct {
	client  *gtfsrt.Client
	feed    Feed
	timeout time.Duration
	log     *slog.Logger
	parent  context.Context
}

// New returns a collector for one feed.
//
// One scrape reads one feed, so the metrics carry no network label. The person
// who runs Prometheus names the target, the same way that every other
// multi-target exporter works.
func New(client *gtfsrt.Client, feed Feed, timeout time.Duration, log *slog.Logger) *Collector {
	return &Collector{client: client, feed: feed, timeout: timeout, log: log}
}

// Describe sends the descriptor of every metric that the collector can emit.
//
// It sends every descriptor, including those that this configuration will
// never write. Describe states what the collector can emit, and not what one
// scrape did emit.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range allDescs {
		ch <- desc
	}
}

// WithContext returns a collector that stops its request when the context
// ends. Use it to tie one scrape to the life of its HTTP request.
func (c *Collector) WithContext(ctx context.Context) prometheus.Collector {
	copied := *c
	copied.parent = ctx
	return &copied
}

// Collect reads the feed and writes its metrics.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	parent := c.parent
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()

	snapshot, err := c.client.Fetch(ctx, c.feed.URL, gtfsrt.FetchOptions{
		Headers:        c.feed.Headers,
		RequestTimeout: c.feed.RequestTimeout,
		NoKeepAlive:    c.feed.NoKeepAlive,
	})
	if err != nil {
		c.log.Warn("cannot read the feed", "feed", c.feed.Name, "url", c.feed.URL, "error", err)
		gauge(ch, upDesc, 0)
	} else {
		gauge(ch, upDesc, 1)
	}

	// The snapshot is never nil, so the size and the duration of a failed
	// attempt still reach Prometheus.
	gauge(ch, fetchDurationDesc, snapshot.Duration.Seconds())
	if snapshot.Bytes > 0 {
		gauge(ch, feedSizeDesc, float64(snapshot.Bytes))
	}
	if !snapshot.OK() {
		return
	}

	c.collectHeader(ch, snapshot)
	c.collectEntities(ch, snapshot)
}

func (c *Collector) collectHeader(ch chan<- prometheus.Metric, snapshot *gtfsrt.Snapshot) {
	header := snapshot.Header()
	gauge(ch, feedInfoDesc, 1, header.GetGtfsRealtimeVersion(), header.GetIncrementality().String())
	// A feed that states no time gets no series. A zero would read as 1970.
	if header != nil && header.Timestamp != nil {
		gauge(ch, feedTimestampDesc, float64(header.GetTimestamp()))
	}
	if header.GetIncrementality() == gtfs.FeedHeader_DIFFERENTIAL {
		c.log.Warn("the feed is DIFFERENTIAL, so every entity count is a count of changes and not a count of the service",
			"feed", c.feed.Name)
	}
}

// counters holds the aggregates of one scrape. Everything is counted in one
// pass over the entities, because a feed holds up to 22642 stop time updates
// and a second pass costs real time.
type counters struct {
	entities   map[string]float64
	tripRel    map[string]float64
	stopRel    map[string]float64
	perRoute   map[string]float64
	vehicles   map[string]float64
	occupancy  map[string]float64
	congestion map[string]float64
	alerts     map[alertKey]float64
	selectors  map[string]float64
	coverage   map[fieldKey]float64
	seenIDs    map[string]int
	deleted    float64
	noPayload  float64
	notInStat  float64
	noSchedule map[noScheduleReason]float64
	// computed marks that at least one delay came from the timetable rather
	// than from the producer.
	computed  bool
	stated    bool
	routeMeta map[string]gtfsstatic.Route
	// Per-route tallies. They stay empty unless the module asks for them.
	routeDelays    map[string]*observations
	routeVehicles  map[string]float64
	routeAlerts    map[routeEffect]float64
	routeCancelled map[string]float64
	routeLate      map[string]float64
	unresolved     map[string]float64
	// route and routeKnown carry the resolved route of the trip that is being
	// read, from the descriptor at the top of tripUpdate down to the delay at
	// the bottom of it.
	route      string
	routeKnown bool
	// The module settings that the tallies above obey.
	perRouteDelay bool
	lateThreshold float64
	delays        *observations
	ages          *observations
}

type alertKey struct {
	cause    string
	effect   string
	severity string
	active   string
}

// routeEffect keys an alert by the route it names and what it does to it.
type routeEffect struct {
	route  string
	effect string
}

type fieldKey struct {
	entityType string
	field      string
}

func newCounters() *counters {
	return &counters{
		entities:       map[string]float64{},
		tripRel:        map[string]float64{},
		stopRel:        map[string]float64{},
		perRoute:       map[string]float64{},
		vehicles:       map[string]float64{},
		occupancy:      map[string]float64{},
		congestion:     map[string]float64{},
		alerts:         map[alertKey]float64{},
		selectors:      map[string]float64{},
		coverage:       map[fieldKey]float64{},
		seenIDs:        map[string]int{},
		routeMeta:      map[string]gtfsstatic.Route{},
		noSchedule:     map[noScheduleReason]float64{},
		routeDelays:    map[string]*observations{},
		routeVehicles:  map[string]float64{},
		routeAlerts:    map[routeEffect]float64{},
		routeCancelled: map[string]float64{},
		routeLate:      map[string]float64{},
		unresolved:     map[string]float64{},
		delays:         &observations{},
		ages:           &observations{},
	}
}

func (c *Collector) collectEntities(ch chan<- prometheus.Metric, snapshot *gtfsrt.Snapshot) {
	now := snapshot.FetchedAt
	full := snapshot.Header().GetIncrementality() == gtfs.FeedHeader_FULL_DATASET
	count := newCounters()
	count.perRouteDelay = c.feed.PerRoute && c.feed.PerRouteDelay
	count.lateThreshold = c.lateThreshold().Seconds()
	index := c.feed.Static.Index()

	for _, entity := range snapshot.Entities() {
		if id := entity.GetId(); id != "" {
			count.seenIDs[id]++
		}
		if entity.GetIsDeleted() && full {
			count.deleted++
		}
		switch {
		case entity.TripUpdate != nil:
			count.entities["trip_update"]++
			count.tripUpdate(entity.TripUpdate, now, index)
		case entity.Vehicle != nil:
			count.entities["vehicle_position"]++
			count.vehiclePosition(entity.Vehicle, now, index)
		case entity.Alert != nil:
			count.entities["alert"]++
			count.alert(entity.Alert, now, index)
		case entity.Shape != nil:
			count.entities["shape"]++
		case entity.Stop != nil:
			count.entities["stop"]++
		case entity.TripModifications != nil:
			count.entities["trip_modifications"]++
		default:
			count.entities["none"]++
			count.noPayload++
		}
	}

	c.write(ch, count)
}

func (t *counters) tripUpdate(update *gtfs.TripUpdate, now time.Time, index *gtfsstatic.Index) {
	trip := update.GetTrip()
	t.tripRel[trip.GetScheduleRelationship().String()]++

	// The descriptor is required by the specification, but a broken feed can
	// leave it out, and a direct field read of a nil message panics.
	if trip != nil {
		if trip.TripId != nil {
			t.coverage[fieldKey{"trip_update", "trip_id"}]++
		}
		// The coverage metric reports what the producer published, so it
		// counts the field and never the value that the index resolved.
		if trip.RouteId != nil {
			t.coverage[fieldKey{"trip_update", "route_id"}]++
		}
		routeID, route, known := t.resolveRoute(trip, index)
		if known {
			t.perRoute[routeID]++
			if route != nil {
				t.routeMeta[routeID] = *route
			}
			// A route with no cancellation gets no series, so the board can
			// tell "none today" from "this route is not running".
			//
			// DELETED counts as well as CANCELED. tripDelay excludes both, so
			// counting only one would let a deleted trip vanish: excluded from
			// the punctuality and absent from the withdrawn count.
			switch trip.GetScheduleRelationship() {
			case gtfs.TripDescriptor_CANCELED, gtfs.TripDescriptor_DELETED:
				t.routeCancelled[routeID]++
			}
		}
		t.countMissingTrip(trip, index)
		t.route = routeID
		t.routeKnown = known
	}
	if update.Timestamp != nil {
		t.coverage[fieldKey{"trip_update", "timestamp"}]++
	}

	for _, stop := range update.GetStopTimeUpdate() {
		t.stopRel[stop.GetScheduleRelationship().String()]++
	}

	result := tripDelay(update, now, index)
	switch {
	case result.ok:
		t.coverage[fieldKey{"trip_update", "delay"}]++
		t.delays.add(result.seconds)
		if t.routeKnown {
			// The board reads the late count directly, because it must still
			// work when the histogram is off.
			if _, seen := t.routeLate[t.route]; !seen {
				t.routeLate[t.route] = 0
			}
			if result.seconds > t.lateThreshold {
				t.routeLate[t.route]++
			}
			if t.perRouteDelay {
				observed, ok := t.routeDelays[t.route]
				if !ok {
					observed = &observations{}
					t.routeDelays[t.route] = observed
				}
				observed.add(result.seconds)
			}
		}
		if result.computed {
			t.computed = true
		} else {
			t.stated = true
		}
	case result.reason != "":
		t.noSchedule[result.reason]++
	}
}

func (t *counters) vehiclePosition(vehicle *gtfs.VehiclePosition, now time.Time, index *gtfsstatic.Index) {
	// current_status has a default of IN_TRANSIT_TO in the specification, so
	// an absent field is a real reading and not a gap.
	t.vehicles[vehicle.GetCurrentStatus().String()]++

	if vehicle.OccupancyStatus != nil {
		t.occupancy[vehicle.GetOccupancyStatus().String()]++
		t.coverage[fieldKey{"vehicle_position", "occupancy_status"}]++
	}
	if vehicle.CongestionLevel != nil {
		t.congestion[vehicle.GetCongestionLevel().String()]++
	}
	if vehicle.Position != nil {
		t.coverage[fieldKey{"vehicle_position", "position"}]++
	}
	// A vehicle that is not assigned to a trip carries no descriptor at all.
	// A direct field read of a nil message panics, so test the message first.
	if trip := vehicle.GetTrip(); trip != nil {
		if trip.TripId != nil {
			t.coverage[fieldKey{"vehicle_position", "trip_id"}]++
		}
		if trip.RouteId != nil {
			t.coverage[fieldKey{"vehicle_position", "route_id"}]++
		}
	}
	if descriptor := vehicle.GetVehicle(); descriptor != nil && descriptor.Id != nil {
		t.coverage[fieldKey{"vehicle_position", "vehicle_id"}]++
	}
	if id, route, ok := t.resolveRoute(vehicle.GetTrip(), index); ok {
		t.routeVehicles[id]++
		if route != nil {
			t.routeMeta[id] = *route
		}
	}
	if vehicle.Timestamp != nil {
		t.coverage[fieldKey{"vehicle_position", "timestamp"}]++
		t.ages.add(now.Sub(time.Unix(int64(vehicle.GetTimestamp()), 0)).Seconds())
	}
}

func (t *counters) alert(alert *gtfs.Alert, now time.Time, index *gtfsstatic.Index) {
	active := "false"
	if alertActive(alert, now) {
		active = "true"
	}
	t.alerts[alertKey{
		cause:    alert.GetCause().String(),
		effect:   alert.GetEffect().String(),
		severity: alert.GetSeverityLevel().String(),
		active:   active,
	}]++

	if alert.HeaderText != nil {
		t.coverage[fieldKey{"alert", "header_text"}]++
	}
	if alert.Url != nil {
		t.coverage[fieldKey{"alert", "url"}]++
	}

	// routes collects the routes of this one alert. It is a set, because an
	// alert names the same route through many selectors: SNCF publishes 314
	// alerts carrying 14,464 trip selectors, so counting per selector would
	// report the number of affected routes wrong by a factor of forty.
	routes := map[string]struct{}{}
	// widest remembers the broadest scope that named no route, so that a
	// network-wide disruption is still visible somewhere.
	widest := ""

	for _, selector := range alert.GetInformedEntity() {
		// One selector can set several of these at once. MBTA sets route_id,
		// route_type and direction_id together.
		if selector.AgencyId != nil {
			t.selectors["agency_id"]++
		}
		if selector.RouteId != nil {
			t.selectors["route_id"]++
		}
		if selector.RouteType != nil {
			t.selectors["route_type"]++
		}
		if selector.Trip != nil {
			t.selectors["trip"]++
		}
		if selector.StopId != nil {
			t.selectors["stop_id"]++
		}
		if selector.DirectionId != nil {
			t.selectors["direction_id"]++
		}

		switch {
		case selector.RouteId != nil:
			routes[selector.GetRouteId()] = struct{}{}
		case selector.Trip != nil:
			// A trip descriptor is exactly what resolveRoute reads, so the
			// alert path needs no resolver of its own.
			if id, route, ok := t.resolveRoute(selector.Trip, index); ok {
				routes[id] = struct{}{}
				if route != nil {
					t.routeMeta[id] = *route
				}
				break
			}
			// The alert named a trip that the timetable does not hold. That is
			// a different fault from naming nothing, and it is worth its own
			// scope: SNCF writes a short trip identifier in its alerts and the
			// long composite one in trips.txt, so none of its 7,231 alert
			// trips resolves.
			if widest == "" || widest == "stop" {
				widest = "unresolved_trip"
			}
		case selector.AgencyId != nil:
			widest = "agency"
		case selector.RouteType != nil:
			if widest == "" {
				widest = "route_type"
			}
		case selector.StopId != nil:
			// A stop names no route without a reverse index, which this
			// exporter does not build.
			if widest == "" {
				widest = "stop"
			}
		}
	}

	// Only an alert that is in force belongs on a status board.
	if active != "true" {
		return
	}
	effect := alert.GetEffect().String()
	for id := range routes {
		t.routeAlerts[routeEffect{route: id, effect: effect}]++
	}
	if len(routes) == 0 {
		if widest == "" {
			widest = "none"
		}
		t.unresolved[widest]++
	}
}

func (c *Collector) write(ch chan<- prometheus.Metric, count *counters) {
	writeMap(ch, entitiesDesc, count.entities)
	writeMap(ch, tripUpdatesDesc, count.tripRel)
	writeMap(ch, stopTimeUpdatesDesc, count.stopRel)
	writeMap(ch, vehiclesDesc, count.vehicles)
	writeMap(ch, vehicleOccupancyDesc, count.occupancy)
	writeMap(ch, vehicleCongestionDesc, count.congestion)
	writeMap(ch, alertSelectorsDesc, count.selectors)

	writeMap(ch, alertsUnresolvedDesc, count.unresolved)

	if c.feed.PerRoute {
		writeMap(ch, routeTripUpdatesDesc, count.perRoute)
		writeMap(ch, routeVehiclesDesc, count.routeVehicles)
		writeMap(ch, routeCancelledDesc, count.routeCancelled)

		threshold := strconv.FormatInt(int64(c.lateThreshold().Seconds()), 10)
		for id, value := range count.routeLate {
			gauge(ch, routeLateDesc, value, id, threshold)
		}
		for key, value := range count.routeAlerts {
			gauge(ch, routeAlertsDesc, value, key.route, key.effect)
		}
		if c.feed.PerRouteDelay {
			for id, observed := range count.routeDelays {
				if observed.n == 0 {
					continue
				}
				ch <- observed.metric(routeDelayDesc, c.buckets(), c.feed.NativeHistogram, id)
			}
		}
	}

	for key, value := range count.alerts {
		gauge(ch, alertsDesc, value, key.cause, key.effect, key.severity, key.active)
	}
	for key, value := range count.coverage {
		gauge(ch, entitiesWithFieldDesc, value, key.entityType, key.field)
	}

	duplicates := float64(0)
	for _, seen := range count.seenIDs {
		if seen > 1 {
			duplicates++
		}
	}
	gauge(ch, duplicateEntityIDsDesc, duplicates)
	gauge(ch, entitiesWithoutPayloadDesc, count.noPayload)
	gauge(ch, entitiesDeletedDesc, count.deleted)
	c.writeStatic(ch, count)

	for reason, value := range count.noSchedule {
		gauge(ch, tripsWithoutScheduleDesc, value, string(reason))
	}
	if count.stated {
		gauge(ch, delaySourceDesc, 1, "producer")
	}
	if count.computed {
		gauge(ch, delaySourceDesc, 1, "computed")
	}

	// A feed that states no delay gets no histogram. Rennes and MBTA both
	// publish absolute times and no delay, so an empty histogram would claim
	// a punctuality that nobody reported.
	if count.delays.n > 0 {
		ch <- count.delays.metric(tripDelayDesc, c.buckets(), c.feed.NativeHistogram)
	}
	if count.ages.n > 0 {
		ch <- count.ages.metric(vehicleAgeDesc, defaultAgeBuckets, c.feed.NativeHistogram)
	}
}

var defaultAgeBuckets = []float64{0, 5, 15, 30, 60, 120, 300, 600, 1800}

// DefaultLateThreshold is the delay past which a trip counts as late. Five
// minutes is the figure that most operators publish against.
const DefaultLateThreshold = 5 * time.Minute

func (c *Collector) lateThreshold() time.Duration {
	if c.feed.LateThreshold > 0 {
		return c.feed.LateThreshold
	}
	return DefaultLateThreshold
}

func (c *Collector) buckets() []float64 {
	if len(c.feed.DelayBuckets) > 0 {
		return c.feed.DelayBuckets
	}
	return defaultDelayBuckets
}

// alertActive reports whether the alert is in force at the given time.
//
// The specification deprecated active_period and replaced it with
// communication_period and impact_period, so read the newer fields first.
// Every feed that this exporter was tested against still sends the deprecated
// field. An alert with no period at all is active, because the specification
// shows it "as long as it appears in the feed".
func alertActive(alert *gtfs.Alert, now time.Time) bool {
	periods := alert.GetImpactPeriod()
	if len(periods) == 0 {
		periods = alert.GetCommunicationPeriod()
	}
	if len(periods) == 0 {
		periods = alert.GetActivePeriod()
	}
	if len(periods) == 0 {
		return true
	}
	stamp := uint64(now.Unix())
	for _, period := range periods {
		if period.Start != nil && stamp < period.GetStart() {
			continue
		}
		if period.End != nil && stamp > period.GetEnd() {
			continue
		}
		return true
	}
	return false
}

// tripDelay returns the one delay of a trip, in seconds.
//
// A cancelled trip has no delay. A stop that the producer marked SKIPPED or
// NO_DATA carries no prediction, so it never contributes.
//
// The order below follows what real feeds publish. Trenitalia sets the
// trip-level field. SNCF, Nantes and Clermont set the field on each stop.
// Rennes and MBTA set neither, and no delay is possible for them without the
// scheduled times of the static feed.
func tripDelay(update *gtfs.TripUpdate, now time.Time, index *gtfsstatic.Index) delayResult {
	switch update.GetTrip().GetScheduleRelationship() {
	case gtfs.TripDescriptor_CANCELED, gtfs.TripDescriptor_DELETED:
		return delayResult{}
	}

	if update.Delay != nil {
		return delayResult{seconds: float64(update.GetDelay()), ok: true}
	}

	stamp := now.Unix()
	fallback := math.NaN()

	// Stop time updates are sparse and ordered by stop_sequence. A trip-update
	// feed carries no current_stop_sequence, so the next stop is the first one
	// whose time has not passed.
	for _, stop := range update.GetStopTimeUpdate() {
		switch stop.GetScheduleRelationship() {
		case gtfs.TripUpdate_StopTimeUpdate_SKIPPED, gtfs.TripUpdate_StopTimeUpdate_NO_DATA:
			continue
		}
		for _, event := range []*gtfs.TripUpdate_StopTimeEvent{stop.GetDeparture(), stop.GetArrival()} {
			delay, ok := eventDelay(event)
			if !ok {
				continue
			}
			if event.Time != nil && event.GetTime() >= stamp {
				return delayResult{seconds: delay, ok: true}
			}
			if math.IsNaN(fallback) {
				fallback = delay
			}
		}
	}
	if !math.IsNaN(fallback) {
		return delayResult{seconds: fallback, ok: true}
	}
	// Last resort: the producer stated no delay anywhere, so measure the
	// predicted times against the static timetable. Rennes and MBTA need this,
	// and nothing else gives them a delay at all.
	if index.Schedule() == nil {
		return delayResult{}
	}
	seconds, reason, ok := scheduleDelay(update, now, index)
	return delayResult{seconds: seconds, computed: ok, reason: reason, ok: ok}
}

// delayResult is the outcome of one attempt to time a trip.
//
// computed separates a delay that this exporter worked out from the timetable
// from one that the producer stated. The two differ in reliability, and
// gtfs_rt_delay_source_info reports which a target used.
type delayResult struct {
	seconds  float64
	computed bool
	reason   noScheduleReason
	ok       bool
}

// eventDelay reads the delay of one stop time event.
//
// A feed may state the delay directly, or state both the predicted time and
// the scheduled time. The second form is newer, and it lets a feed report a
// delay without any static schedule.
func eventDelay(event *gtfs.TripUpdate_StopTimeEvent) (float64, bool) {
	if event == nil {
		return 0, false
	}
	if event.Delay != nil {
		return float64(event.GetDelay()), true
	}
	if event.Time != nil && event.ScheduledTime != nil {
		return float64(event.GetTime() - event.GetScheduledTime()), true
	}
	return 0, false
}

// observations gathers the values of one histogram during a scrape.
type observations struct {
	values []float64
	sum    float64
	n      uint64
}

func (o *observations) add(value float64) {
	o.values = append(o.values, value)
	o.sum += value
	o.n++
}

// metric builds either a classic or a native histogram.
//
// A constant native histogram carries no classic buckets, so one metric is one
// form or the other. Native buckets are exponential, and the median delay of a
// real feed is exactly 0, so the classic form stays the default.
func (o *observations) metric(desc *prometheus.Desc, bounds []float64, native bool, labels ...string) prometheus.Metric {
	if native {
		positive, negative, zero := o.nativeBuckets()
		return prometheus.MustNewConstNativeHistogram(desc, o.n, o.sum,
			positive, negative, zero, nativeSchema, nativeZeroThreshold, time.Time{}, labels...)
	}
	sort.Float64s(o.values)
	buckets := make(map[float64]uint64, len(bounds))
	for _, bound := range bounds {
		buckets[bound] = uint64(sort.SearchFloat64s(o.values, math.Nextafter(bound, math.Inf(1))))
	}
	return prometheus.MustNewConstHistogram(desc, o.n, o.sum, buckets, labels...)
}

// nativeBuckets sorts the values into exponential buckets. Index 0 holds the
// values whose upper bound is 1, as the client library requires, and the sum
// of every bucket must equal the count.
func (o *observations) nativeBuckets() (positive, negative map[int]int64, zero uint64) {
	positive = map[int]int64{}
	negative = map[int]int64{}
	factor := math.Exp2(nativeSchema)
	for _, value := range o.values {
		size := math.Abs(value)
		if size <= nativeZeroThreshold {
			zero++
			continue
		}
		index := int(math.Ceil(math.Log2(size) * factor))
		if value > 0 {
			positive[index]++
		} else {
			negative[index]++
		}
	}
	return positive, negative, zero
}

func writeMap(ch chan<- prometheus.Metric, desc *prometheus.Desc, values map[string]float64) {
	for label, value := range values {
		gauge(ch, desc, value, label)
	}
}

func gauge(ch chan<- prometheus.Metric, desc *prometheus.Desc, value float64, labels ...string) {
	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
}

// resolveRoute returns the route of a trip descriptor.
//
// A feed often names the trip and no route. SNCF publishes 1140 trip updates
// and no route identifier on any of them, so the identifier comes from the
// static index instead. The metadata is returned beside it, for the info
// metric that the counting metric joins.
func (t *counters) resolveRoute(trip *gtfs.TripDescriptor, index *gtfsstatic.Index) (string, *gtfsstatic.Route, bool) {
	if trip == nil {
		return "", nil, false
	}
	if trip.RouteId != nil {
		id := trip.GetRouteId()
		if route, ok := index.Route(id); ok {
			return id, &route, true
		}
		return id, nil, true
	}
	if trip.TripId == nil {
		return "", nil, false
	}
	route, ok := index.RouteOfTrip(trip.GetTripId())
	if !ok {
		return "", nil, false
	}
	return route.ID, &route, true
}

// countMissingTrip counts a trip that the static feed does not hold.
//
// Two kinds of trip are left out. A trip marked ADDED, NEW or DUPLICATED has
// no static row by design, and a trip that carries no identifier cannot be
// looked up at all, because the descriptor may name the trip by route,
// direction and start time instead.
func (t *counters) countMissingTrip(trip *gtfs.TripDescriptor, index *gtfsstatic.Index) {
	if index == nil || trip.TripId == nil {
		return
	}
	switch trip.GetScheduleRelationship() {
	case gtfs.TripDescriptor_ADDED, gtfs.TripDescriptor_NEW, gtfs.TripDescriptor_DUPLICATED:
		return
	}
	if _, ok := index.RouteOfTrip(trip.GetTripId()); !ok {
		t.notInStat++
	}
}

// writeStatic reports the state of the static index.
//
// The series repeat under every target that shares one static feed. That is
// deliberate. A reader who looks at one target must see whether the index that
// fills its labels is ready.
func (c *Collector) writeStatic(ch chan<- prometheus.Metric, count *counters) {
	if c.feed.Static == nil {
		return
	}
	index := c.feed.Static.Index()
	if index == nil {
		gauge(ch, staticLoadedDesc, 0)
		return
	}
	gauge(ch, staticLoadedDesc, 1)
	gauge(ch, tripsNotInStaticDesc, count.notInStat)

	info := index.FeedInfo()
	gauge(ch, staticInfoDesc, 1, info.Version, info.StartDate, info.EndDate)
	gauge(ch, staticLoadTimestampDesc, float64(index.LoadedAt().Unix()))
	gauge(ch, staticLoadDurationDesc, index.Took().Seconds())
	gauge(ch, staticBytesDesc, float64(index.Bytes()))

	agencies, routes, trips := index.Stats()
	gauge(ch, staticAgenciesDesc, float64(agencies))
	gauge(ch, staticRoutesDesc, float64(routes))
	gauge(ch, staticTripsDesc, float64(trips))

	// The row count is absent when the module asked for no timetable, and 0
	// when it asked and the timetable is missing, so that == 0 stays a usable
	// alert.
	if c.feed.Schedule {
		gauge(ch, staticScheduleRowsDesc, float64(index.Schedule().Rows()))
	}
	inService := float64(0)
	if index.InService(time.Now()) {
		inService = 1
	}
	gauge(ch, staticInServiceDesc, inService)
	if expires, ok := index.ExpiresAt(); ok {
		gauge(ch, staticExpiresDesc, float64(expires.Unix()))
	}
	pending := float64(0)
	if c.feed.Static.Pending() {
		pending = 1
	}
	gauge(ch, staticPendingDesc, pending)

	// The info series exist to be joined with gtfs_rt_route_trip_updates, so
	// they follow the same flag. Without the gate, a module that leaves
	// per_route off still published one series per route with nothing to join
	// to: 460 of them for SNCF.
	if !c.feed.PerRoute {
		return
	}
	// Only the routes that this feed names get an info series. A network with
	// 1973 routes must not publish every one of them on a feed that runs ten.
	for id, route := range count.routeMeta {
		gauge(ch, staticRouteInfoDesc, 1, id, route.ShortName, route.Type, route.AgencyID)
	}
}
