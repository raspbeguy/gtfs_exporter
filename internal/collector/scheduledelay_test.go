package collector_test

import (
	"archive/zip"
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/proto"

	"github.com/raspbeguy/gtfs_exporter/internal/collector"
	"github.com/raspbeguy/gtfs_exporter/internal/gtfsstatic"
)

// timetableLoader serves a static feed that carries a timetable.
func timetableLoader(t *testing.T, members map[string]string) *gtfsstatic.Loader {
	t.Helper()
	base := map[string]string{
		"agency.txt": "agency_id,agency_name,agency_timezone\nA,Agency,Europe/Paris\n",
		"routes.txt": "route_id,agency_id,route_short_name,route_type\nr1,A,a,1\n",
		"trips.txt":  "route_id,service_id,trip_id\nr1,s1,t1\n",
	}
	for name, body := range members {
		base[name] = body
	}
	buffer := &bytes.Buffer{}
	writer := zip.NewWriter(buffer)
	for name, body := range base {
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
		gtfsstatic.Feed{URL: server.URL, RefreshInterval: time.Hour, Schedule: true},
		server.Client(), slog.New(slog.DiscardHandler))
	loader.Start(t.Context())
	deadline := time.Now().Add(10 * time.Second)
	for loader.Index() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if loader.Index() == nil {
		t.Fatal("the timetable never became ready")
	}
	return loader
}

// stopTimesAround writes a timetable whose stops sit near the current clock,
// so that a test does not depend on the hour at which it runs.
func stopTimesAround(t *testing.T, offsets ...time.Duration) (string, []int64) {
	t.Helper()
	paris, err := time.LoadLocation("Europe/Paris")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().In(paris)
	origin := gtfsstatic.ServiceDayOrigin(now, paris)

	body := "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
	var scheduled []int64
	for i, offset := range offsets {
		at := now.Add(offset)
		seconds := int(at.Sub(origin).Seconds())
		clock := fmt.Sprintf("%02d:%02d:%02d", seconds/3600, seconds/60%60, seconds%60)
		body += fmt.Sprintf("t1,%s,%s,s%d,%d\n", clock, clock, i+1, i+1)
		scheduled = append(scheduled, origin.Add(time.Duration(seconds)*time.Second).Unix())
	}
	return body, scheduled
}

func tripWithStops(stops ...*gtfs.TripUpdate_StopTimeUpdate) *gtfs.FeedMessage {
	return &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{
			GtfsRealtimeVersion: proto.String("2.0"),
			Incrementality:      gtfs.FeedHeader_FULL_DATASET.Enum(),
			Timestamp:           proto.Uint64(uint64(time.Now().Unix())),
		},
		Entity: []*gtfs.FeedEntity{{
			Id: proto.String("e1"),
			TripUpdate: &gtfs.TripUpdate{
				Trip:           &gtfs.TripDescriptor{TripId: proto.String("t1")},
				StopTimeUpdate: stops,
			},
		}},
	}
}

// TestAStopSequenceJoinMatchesTheMBTAShape checks the exact join. MBTA sends
// stop_sequence on all 24,095 of its stop time updates.
func TestAStopSequenceJoinMatchesTheMBTAShape(t *testing.T) {
	times, scheduled := stopTimesAround(t, -10*time.Minute, 10*time.Minute)
	loader := timetableLoader(t, map[string]string{"stop_times.txt": times})

	// The next stop runs 90 seconds late.
	message := tripWithStops(&gtfs.TripUpdate_StopTimeUpdate{
		StopSequence: proto.Uint32(2),
		Departure:    &gtfs.TripUpdate_StopTimeEvent{Time: proto.Int64(scheduled[1] + 90)},
	})
	server := serveFeed(t, message)
	subject := newCollector(t, server.URL, collector.Feed{Schedule: true, Static: loader})

	count, sum := histogramOf(t, subject, "gtfs_rt_trip_delay_seconds")
	if count != 1 || sum != 90 {
		t.Errorf("got count %d and sum %v, want 1 and 90", count, sum)
	}
	expected := `
# HELP gtfs_rt_delay_source_info Where the delay of this feed comes from. The value is always 1. A producer value is what the operator published. A computed value is what this exporter worked out from the static timetable, which is a weaker reading.
# TYPE gtfs_rt_delay_source_info gauge
gtfs_rt_delay_source_info{source="computed"} 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_delay_source_info"); err != nil {
		t.Error(err)
	}
}

// TestAStopIDJoinMatchesTheRennesShape checks the other join. Rennes sends no
// stop_sequence at all, only a stop identifier.
func TestAStopIDJoinMatchesTheRennesShape(t *testing.T) {
	times, scheduled := stopTimesAround(t, -10*time.Minute, 10*time.Minute)
	loader := timetableLoader(t, map[string]string{"stop_times.txt": times})

	message := tripWithStops(&gtfs.TripUpdate_StopTimeUpdate{
		StopId:    proto.String("s2"),
		Departure: &gtfs.TripUpdate_StopTimeEvent{Time: proto.Int64(scheduled[1] - 45)},
	})
	server := serveFeed(t, message)
	subject := newCollector(t, server.URL, collector.Feed{Schedule: true, Static: loader})

	count, sum := histogramOf(t, subject, "gtfs_rt_trip_delay_seconds")
	if count != 1 || sum != -45 {
		t.Errorf("got count %d and sum %v, want 1 and -45 for an early trip", count, sum)
	}
}

// TestALoopRouteWithNoSequenceObservesNothing checks the refusal.
//
// A loop route calls at the same stop twice, and MBTA does this on 209 trips.
// With only a stop identifier there is nothing to tell the two calls apart, so
// the exporter must publish nothing rather than pick one at random.
func TestALoopRouteWithNoSequenceObservesNothing(t *testing.T) {
	paris, _ := time.LoadLocation("Europe/Paris")
	now := time.Now().In(paris)
	origin := gtfsstatic.ServiceDayOrigin(now, paris)
	seconds := int(now.Add(10 * time.Minute).Sub(origin).Seconds())
	clock := fmt.Sprintf("%02d:%02d:%02d", seconds/3600, seconds/60%60, seconds%60)

	loader := timetableLoader(t, map[string]string{
		"stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n" +
			fmt.Sprintf("t1,%s,%s,loop,1\n", clock, clock) +
			fmt.Sprintf("t1,%s,%s,loop,2\n", clock, clock),
	})
	message := tripWithStops(&gtfs.TripUpdate_StopTimeUpdate{
		StopId:    proto.String("loop"),
		Departure: &gtfs.TripUpdate_StopTimeEvent{Time: proto.Int64(now.Add(11 * time.Minute).Unix())},
	})
	server := serveFeed(t, message)
	subject := newCollector(t, server.URL, collector.Feed{Schedule: true, Static: loader})

	if got := testutil.CollectAndCount(subject, "gtfs_rt_trip_delay_seconds"); got != 0 {
		t.Errorf("got %d delay series for an ambiguous stop, want 0", got)
	}
}

// TestADepartureIsComparedAgainstAScheduledDeparture guards the dwell trap.
//
// Comparing a predicted departure against a scheduled arrival adds the dwell
// time to the delay. Dwell is zero on 99.9% of the Rennes rows, but on those
// that dwell the median is 300 seconds and the longest is 1740.
func TestADepartureIsComparedAgainstAScheduledDeparture(t *testing.T) {
	paris, _ := time.LoadLocation("Europe/Paris")
	now := time.Now().In(paris)
	origin := gtfsstatic.ServiceDayOrigin(now, paris)
	arrive := int(now.Add(10 * time.Minute).Sub(origin).Seconds())
	depart := arrive + 300 // a five minute layover
	stamp := func(s int) string { return fmt.Sprintf("%02d:%02d:%02d", s/3600, s/60%60, s%60) }

	loader := timetableLoader(t, map[string]string{
		"stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n" +
			fmt.Sprintf("t1,%s,%s,s1,1\n", stamp(arrive), stamp(depart)),
	})
	// The vehicle departs exactly on time. Against the arrival it would look
	// 300 seconds late.
	message := tripWithStops(&gtfs.TripUpdate_StopTimeUpdate{
		StopSequence: proto.Uint32(1),
		Departure:    &gtfs.TripUpdate_StopTimeEvent{Time: proto.Int64(origin.Add(time.Duration(depart) * time.Second).Unix())},
	})
	server := serveFeed(t, message)
	subject := newCollector(t, server.URL, collector.Feed{Schedule: true, Static: loader})

	count, sum := histogramOf(t, subject, "gtfs_rt_trip_delay_seconds")
	if count != 1 || sum != 0 {
		t.Errorf("got count %d and sum %v, want 1 and 0, not the 300 s of dwell", count, sum)
	}
}

// TestAnExpiredDatasetStopsTheDelayButKeepsTheRoute checks the rule with the
// shortest fuse.
//
// A dataset lives one to two weeks. A timetable outside its window describes a
// service that no longer runs, so a delay measured against it is wrong rather
// than missing.
func TestAnExpiredDatasetStopsTheDelayButKeepsTheRoute(t *testing.T) {
	times, scheduled := stopTimesAround(t, 10*time.Minute)
	loader := timetableLoader(t, map[string]string{
		"stop_times.txt": times,
		"feed_info.txt": "feed_publisher_name,feed_version,feed_start_date,feed_end_date\n" +
			"Agency,v1,20200101,20200131\n",
	})
	message := tripWithStops(&gtfs.TripUpdate_StopTimeUpdate{
		StopSequence: proto.Uint32(1),
		Departure:    &gtfs.TripUpdate_StopTimeEvent{Time: proto.Int64(scheduled[0] + 60)},
	})
	server := serveFeed(t, message)
	subject := newCollector(t, server.URL, collector.Feed{PerRoute: true, Schedule: true, Static: loader})

	if got := testutil.CollectAndCount(subject, "gtfs_rt_trip_delay_seconds"); got != 0 {
		t.Errorf("got %d delay series from an expired dataset, want 0", got)
	}
	expected := `
# HELP gtfs_static_in_service 1 when the service day falls inside the validity window of the dataset. The exporter computes no delay while this is 0, because a timetable that has expired gives a wrong answer rather than no answer.
# TYPE gtfs_static_in_service gauge
gtfs_static_in_service 0
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_static_in_service"); err != nil {
		t.Error(err)
	}
	// The route still resolves. It ages gracefully, and the timing does not.
	if got := testutil.CollectAndCount(subject, "gtfs_rt_route_trip_updates"); got != 1 {
		t.Errorf("got %d route series, want the route to survive the expiry", got)
	}
	reason := `
# HELP gtfs_rt_trips_without_schedule Live trips that the timetable cannot time, by reason. Several reasons are healthy: no_timeable_stop is an update that skips a stop or two on a trip which is still running, and MBTA sends about 40% of its trip updates that way. Never alert on the total without splitting the label.
# TYPE gtfs_rt_trips_without_schedule gauge
gtfs_rt_trips_without_schedule{reason="dataset_expired"} 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(reason), "gtfs_rt_trips_without_schedule"); err != nil {
		t.Error(err)
	}
}

// TestATripOfOnlySkippedStopsIsReportedAsSuch checks the reason label.
//
// MBTA sends about 45% of its trip updates as stop-cancellation notices: every
// stop is SKIPPED and none carries a prediction. That is healthy, and it must
// not read as a failed join.
func TestATripOfOnlySkippedStopsIsReportedAsSuch(t *testing.T) {
	times, _ := stopTimesAround(t, 10*time.Minute)
	loader := timetableLoader(t, map[string]string{"stop_times.txt": times})

	message := tripWithStops(&gtfs.TripUpdate_StopTimeUpdate{
		StopSequence:         proto.Uint32(1),
		ScheduleRelationship: gtfs.TripUpdate_StopTimeUpdate_SKIPPED.Enum(),
	})
	server := serveFeed(t, message)
	subject := newCollector(t, server.URL, collector.Feed{Schedule: true, Static: loader})

	expected := `
# HELP gtfs_rt_trips_without_schedule Live trips that the timetable cannot time, by reason. Several reasons are healthy: no_timeable_stop is an update that skips a stop or two on a trip which is still running, and MBTA sends about 40% of its trip updates that way. Never alert on the total without splitting the label.
# TYPE gtfs_rt_trips_without_schedule gauge
gtfs_rt_trips_without_schedule{reason="no_timeable_stop"} 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_trips_without_schedule"); err != nil {
		t.Error(err)
	}
}

// TestAFrequencyBasedTripObservesNoDelay checks the skip. The stop times of
// such a trip count from the start of the trip, so reading them as absolute
// would report a delay of hours.
func TestAFrequencyBasedTripObservesNoDelay(t *testing.T) {
	times, scheduled := stopTimesAround(t, 10*time.Minute)
	loader := timetableLoader(t, map[string]string{
		"stop_times.txt":  times,
		"frequencies.txt": "trip_id,start_time,end_time,headway_secs\nt1,06:00:00,20:00:00,600\n",
	})
	message := tripWithStops(&gtfs.TripUpdate_StopTimeUpdate{
		StopSequence: proto.Uint32(1),
		Departure:    &gtfs.TripUpdate_StopTimeEvent{Time: proto.Int64(scheduled[0] + 60)},
	})
	server := serveFeed(t, message)
	subject := newCollector(t, server.URL, collector.Feed{Schedule: true, Static: loader})

	if got := testutil.CollectAndCount(subject, "gtfs_rt_trip_delay_seconds"); got != 0 {
		t.Errorf("got %d delay series for a frequency based trip, want 0", got)
	}
	expected := `
# HELP gtfs_rt_trips_without_schedule Live trips that the timetable cannot time, by reason. Several reasons are healthy: no_timeable_stop is an update that skips a stop or two on a trip which is still running, and MBTA sends about 40% of its trip updates that way. Never alert on the total without splitting the label.
# TYPE gtfs_rt_trips_without_schedule gauge
gtfs_rt_trips_without_schedule{reason="frequency_based"} 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_trips_without_schedule"); err != nil {
		t.Error(err)
	}
}

// TestAProducerDelayWinsOverTheTimetable checks the precedence. A number that
// the operator published is a stronger reading than one this exporter derived.
func TestAProducerDelayWinsOverTheTimetable(t *testing.T) {
	times, scheduled := stopTimesAround(t, 10*time.Minute)
	loader := timetableLoader(t, map[string]string{"stop_times.txt": times})

	message := tripWithStops(&gtfs.TripUpdate_StopTimeUpdate{
		StopSequence: proto.Uint32(1),
		Departure: &gtfs.TripUpdate_StopTimeEvent{
			Time:  proto.Int64(scheduled[0] + 600),
			Delay: proto.Int32(42), // the producer disagrees with the arithmetic
		},
	})
	server := serveFeed(t, message)
	subject := newCollector(t, server.URL, collector.Feed{Schedule: true, Static: loader})

	count, sum := histogramOf(t, subject, "gtfs_rt_trip_delay_seconds")
	if count != 1 || sum != 42 {
		t.Errorf("got count %d and sum %v, want the stated 42", count, sum)
	}
	expected := `
# HELP gtfs_rt_delay_source_info Where the delay of this feed comes from. The value is always 1. A producer value is what the operator published. A computed value is what this exporter worked out from the static timetable, which is a weaker reading.
# TYPE gtfs_rt_delay_source_info gauge
gtfs_rt_delay_source_info{source="producer"} 1
`
	if err := testutil.CollectAndCompare(subject, strings.NewReader(expected), "gtfs_rt_delay_source_info"); err != nil {
		t.Error(err)
	}
}
