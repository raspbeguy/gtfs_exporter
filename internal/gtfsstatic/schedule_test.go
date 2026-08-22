package gtfsstatic_test

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

	"github.com/raspbeguy/gtfs_exporter/internal/gtfsstatic"
)

// scheduleFixture builds a zip with the members that a timetable needs.
func scheduleFixture(t *testing.T, members map[string]string) []byte {
	t.Helper()
	base := map[string]string{
		"agency.txt": "agency_id,agency_name,agency_timezone\nSTAR,Star,Europe/Paris\n",
		"routes.txt": "route_id,route_short_name,route_type\nr1,a,1\n",
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
	return buffer.Bytes()
}

// loadWithSchedule serves the fixture and returns a ready index.
func loadWithSchedule(t *testing.T, body []byte, feed gtfsstatic.Feed) *gtfsstatic.Index {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	feed.URL = server.URL
	if feed.RefreshInterval == 0 {
		feed.RefreshInterval = time.Hour
	}
	loader := gtfsstatic.NewLoader(feed, server.Client(), slog.New(slog.DiscardHandler))
	loader.Start(t.Context())
	deadline := time.Now().Add(10 * time.Second)
	for loader.Index() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if loader.Index() == nil {
		t.Fatal("the index never became ready")
	}
	return loader.Index()
}

const threeStops = `trip_id,arrival_time,departure_time,stop_id,stop_sequence
t1,08:00:00,08:00:00,s1,1
t1,08:10:00,08:12:00,s2,2
t1,08:20:00,08:20:00,s3,3
`

// TestTheScheduleIsAbsentWhenNotAskedFor checks that stop_times.txt costs
// nothing for a module that did not ask for a timetable.
func TestTheScheduleIsAbsentWhenNotAskedFor(t *testing.T) {
	body := scheduleFixture(t, map[string]string{"stop_times.txt": threeStops})
	index := loadWithSchedule(t, body, gtfsstatic.Feed{})

	if index.Schedule() != nil {
		t.Error("the schedule must be nil when the module did not ask for one")
	}
	if rows := index.Schedule().Rows(); rows != 0 {
		t.Errorf("got %d rows, want 0", rows)
	}
}

func TestTheScheduleIsBuiltWhenAskedFor(t *testing.T) {
	body := scheduleFixture(t, map[string]string{"stop_times.txt": threeStops})
	index := loadWithSchedule(t, body, gtfsstatic.Feed{Schedule: true})

	schedule := index.Schedule()
	if schedule.Rows() != 3 {
		t.Fatalf("got %d rows, want 3", schedule.Rows())
	}
	arrival, departure, ok := schedule.BySequence("t1", 2)
	if !ok {
		t.Fatal("stop 2 must be in the schedule")
	}
	if arrival != 8*3600+600 || departure != 8*3600+720 {
		t.Errorf("got arrival %d and departure %d, want 29400 and 29520", arrival, departure)
	}
	if _, _, ok := schedule.BySequence("t1", 9); ok {
		t.Error("a sequence that the trip does not hold must not be found")
	}
	if _, _, ok := schedule.BySequence("absent", 1); ok {
		t.Error("a trip that the feed does not hold must not be found")
	}
}

// TestAScheduleTimePastMidnightIsCorrect checks the representation.
//
// MBTA writes a time at or past 24:00:00 in 4.7% of its rows, and Rennes in
// 1.1%. Seconds from the origin of the service day hold such a value. A clock
// time does not.
func TestAScheduleTimePastMidnightIsCorrect(t *testing.T) {
	body := scheduleFixture(t, map[string]string{"stop_times.txt": `trip_id,arrival_time,departure_time,stop_id,stop_sequence
t1,23:50:00,23:50:00,s1,1
t1,25:30:00,25:30:00,s2,2
`})
	index := loadWithSchedule(t, body, gtfsstatic.Feed{Schedule: true})

	_, departure, ok := index.Schedule().BySequence("t1", 2)
	if !ok {
		t.Fatal("the late stop must be in the schedule")
	}
	if want := int32(25*3600 + 30*60); departure != want {
		t.Errorf("got %d seconds, want %d", departure, want)
	}
}

// TestALoopRouteReturnsEveryCallAtTheStop guards the join that Rennes forces.
//
// MBTA repeats a stop inside one trip on 221 rows across 209 trips. A lookup
// that returned one call would attach the delay to the wrong one.
func TestALoopRouteReturnsEveryCallAtTheStop(t *testing.T) {
	body := scheduleFixture(t, map[string]string{"stop_times.txt": `trip_id,arrival_time,departure_time,stop_id,stop_sequence
t1,08:00:00,08:00:00,loop,1
t1,08:10:00,08:10:00,other,2
t1,08:20:00,08:20:00,loop,3
`})
	index := loadWithSchedule(t, body, gtfsstatic.Feed{Schedule: true})

	calls := index.Schedule().ByStop("t1", "loop")
	if len(calls) != 2 {
		t.Fatalf("got %d calls at the repeated stop, want 2", len(calls))
	}
	if calls[0].Sequence != 1 || calls[1].Sequence != 3 {
		t.Errorf("got sequences %d and %d, want 1 and 3", calls[0].Sequence, calls[1].Sequence)
	}
	if got := index.Schedule().ByStop("t1", "nowhere"); got != nil {
		t.Errorf("got %v for an unknown stop, want nothing", got)
	}
}

// TestAStopTimeWithNoTimeIsSkipped checks the blank that GTFS allows at a stop
// which is not a timepoint.
func TestAStopTimeWithNoTimeIsSkipped(t *testing.T) {
	body := scheduleFixture(t, map[string]string{"stop_times.txt": `trip_id,arrival_time,departure_time,stop_id,stop_sequence
t1,08:00:00,08:00:00,s1,1
t1,,,s2,2
`})
	index := loadWithSchedule(t, body, gtfsstatic.Feed{Schedule: true})

	arrival, departure, ok := index.Schedule().BySequence("t1", 2)
	if !ok {
		t.Fatal("the row must still be in the schedule")
	}
	if arrival != -1 || departure != -1 {
		t.Errorf("got arrival %d and departure %d, want -1 for both", arrival, departure)
	}
}

// TestAnUnsortedStopTimesFileStillBuilds checks that the builder sorts.
//
// GTFS files are conventionally sorted by trip and sequence, but the
// specification does not require it.
func TestAnUnsortedStopTimesFileStillBuilds(t *testing.T) {
	body := scheduleFixture(t, map[string]string{
		"trips.txt": "route_id,service_id,trip_id\nr1,s1,t1\nr1,s1,t2\n",
		"stop_times.txt": `trip_id,arrival_time,departure_time,stop_id,stop_sequence
t2,09:00:00,09:00:00,s9,1
t1,08:20:00,08:20:00,s3,3
t2,09:10:00,09:10:00,s8,2
t1,08:00:00,08:00:00,s1,1
t1,08:10:00,08:10:00,s2,2
`})
	index := loadWithSchedule(t, body, gtfsstatic.Feed{Schedule: true})
	schedule := index.Schedule()

	for _, want := range []struct {
		trip     string
		sequence uint32
		seconds  int32
	}{
		{"t1", 1, 8 * 3600},
		{"t1", 2, 8*3600 + 600},
		{"t1", 3, 8*3600 + 1200},
		{"t2", 1, 9 * 3600},
		{"t2", 2, 9*3600 + 600},
	} {
		_, departure, ok := schedule.BySequence(want.trip, want.sequence)
		if !ok || departure != want.seconds {
			t.Errorf("%s stop %d: got %d and %v, want %d", want.trip, want.sequence, departure, ok, want.seconds)
		}
	}
}

// TestAFrequencyBasedTripIsMarked checks the detection.
//
// The stop times of such a trip count from the start of the trip and not from
// the service day, so a delay computed from them would be hours out.
func TestAFrequencyBasedTripIsMarked(t *testing.T) {
	body := scheduleFixture(t, map[string]string{
		"trips.txt":      "route_id,service_id,trip_id\nr1,s1,t1\nr1,s1,t2\n",
		"stop_times.txt": threeStops + "t2,00:00:00,00:00:00,s1,1\n",
		"frequencies.txt": "trip_id,start_time,end_time,headway_secs\n" +
			"t2,06:00:00,20:00:00,600\n",
	})
	index := loadWithSchedule(t, body, gtfsstatic.Feed{Schedule: true})

	if index.Schedule().FrequencyBased("t1") {
		t.Error("t1 is not frequency based")
	}
	if !index.Schedule().FrequencyBased("t2") {
		t.Error("t2 is listed in frequencies.txt and must be marked")
	}
}

// TestTheRowGuardStillLoadsTheRouteIndex checks the refusal.
//
// A timetable that does not fit must not take the route index down with it.
// The routes are small and useful on their own.
func TestTheRowGuardStillLoadsTheRouteIndex(t *testing.T) {
	var big strings.Builder
	big.WriteString("trip_id,arrival_time,departure_time,stop_id,stop_sequence\n")
	for i := range 50 {
		fmt.Fprintf(&big, "t1,08:00:%02d,08:00:%02d,s%d,%d\n", i%60, i%60, i, i+1)
	}
	body := scheduleFixture(t, map[string]string{"stop_times.txt": big.String()})
	index := loadWithSchedule(t, body, gtfsstatic.Feed{Schedule: true, MaxScheduleRows: 10})

	if index.Schedule() != nil {
		t.Error("the schedule must be refused when it passes the guard")
	}
	if _, _, trips := index.Stats(); trips != 1 {
		t.Errorf("got %d trips, want the route index to load anyway", trips)
	}
	if route, ok := index.Route("r1"); !ok || route.ShortName != "a" {
		t.Error("the routes must still answer after a refused schedule")
	}
}
