package gtfsstatic

import (
	"archive/zip"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// DefaultMaxScheduleRows limits the size of a schedule.
//
// Rennes holds 1.0 million rows and MBTA 2.2 million, so the default leaves
// room for a network twice the size of MBTA. Ile-de-France holds 15.1 million
// and would be refused, which is correct: it publishes no realtime feed.
const DefaultMaxScheduleRows = 5_000_000

// noTime marks a stop time that the feed left blank. GTFS allows a blank time
// at a stop that is not a timepoint.
const noTime int32 = -1

// Schedule is the timetable of one dataset.
//
// It is immutable once built, so it rides inside the Index that the loader
// swaps atomically, and a reader needs no lock.
type Schedule struct {
	// rows is sorted by trip and then by stop_sequence.
	rows []scheduledStop
	// trips interns the trip identifiers, and spans is parallel to it. MBTA
	// repeats 89,080 identifiers across 2.2 million rows, so a string in every
	// row, and a second map keyed by string, cost more than the rows.
	tripByID map[string]uint32
	spans    []span
	// stops interns the stop identifiers. MBTA repeats 10,297 of them across
	// 2.2 million rows, so a string in every row would cost more than the rows
	// themselves.
	stops    []string
	stopByID map[string]uint32
	// frequencyBased holds the trips of frequencies.txt. Their stop times are
	// relative to the start of the trip and not to the service day, so a delay
	// computed from them would be hours out.
	frequencyBased map[string]struct{}
}

// span is a window into Schedule.rows.
type span struct{ start, end uint32 }

// scheduledStop is one row of stop_times.txt, at 16 bytes.
//
// The times count seconds from the origin of the service day, so a value past
// 24:00:00 stays correct. MBTA writes one in 4.7% of its rows.
type scheduledStop struct {
	sequence  uint32
	stop      uint32 // index into Schedule.stops
	arrival   int32
	departure int32
}

// Rows returns the number of stop times that the schedule holds.
func (s *Schedule) Rows() int {
	if s == nil {
		return 0
	}
	return len(s.rows)
}

// FrequencyBased reports whether the stop times of the trip are relative to
// the start of the trip rather than to the service day.
func (s *Schedule) FrequencyBased(tripID string) bool {
	if s == nil {
		return false
	}
	_, ok := s.frequencyBased[tripID]
	return ok
}

// Has reports whether the schedule holds any stop time for the trip.
func (s *Schedule) Has(tripID string) bool {
	if s == nil {
		return false
	}
	_, ok := s.rowsOf(tripID)
	return ok
}

// StopTime returns the scheduled arrival and departure at one call of a trip,
// in seconds from the origin of the service day. A value of noTime means that
// the feed stated none.
//
// bySequence selects the call by stop_sequence, which MBTA sends on every stop
// time update. It is exact.
func (s *Schedule) BySequence(tripID string, sequence uint32) (arrival, departure int32, ok bool) {
	rows, found := s.rowsOf(tripID)
	if !found {
		return 0, 0, false
	}
	// The rows of a trip are sorted by sequence, so a binary search works.
	at := sort.Search(len(rows), func(i int) bool { return rows[i].sequence >= sequence })
	if at == len(rows) || rows[at].sequence != sequence {
		return 0, 0, false
	}
	return rows[at].arrival, rows[at].departure, true
}

// ByStop returns the scheduled times of every call of a trip at one stop.
//
// It returns a slice and not one row, because a loop route calls at the same
// stop twice. MBTA does this on 221 rows across 209 trips. A caller that took
// only the first would attach the delay to the wrong call.
//
// Rennes forces this path, because it sends no stop_sequence at all.
func (s *Schedule) ByStop(tripID, stopID string) []StopTime {
	rows, found := s.rowsOf(tripID)
	if !found {
		return nil
	}
	wanted, ok := s.stopIndex(stopID)
	if !ok {
		return nil
	}
	// A span is ten to thirty rows, so a scan costs nothing and it finds every
	// call rather than one.
	var calls []StopTime
	for _, row := range rows {
		if row.stop == wanted {
			calls = append(calls, StopTime{
				Sequence:  row.sequence,
				Arrival:   row.arrival,
				Departure: row.departure,
			})
		}
	}
	return calls
}

// StopTime is one call of a trip at a stop, in seconds from the origin of the
// service day.
type StopTime struct {
	Sequence  uint32
	Arrival   int32
	Departure int32
}

func (s *Schedule) rowsOf(tripID string) ([]scheduledStop, bool) {
	if s == nil {
		return nil, false
	}
	at, ok := s.tripByID[tripID]
	if !ok {
		return nil, false
	}
	window := s.spans[at]
	if window.end <= window.start {
		return nil, false
	}
	return s.rows[window.start:window.end], true
}

func (s *Schedule) internTrip(tripID string) uint32 {
	if index, ok := s.tripByID[tripID]; ok {
		return index
	}
	index := uint32(len(s.tripByID))
	s.tripByID[tripID] = index
	return index
}

func (s *Schedule) stopIndex(stopID string) (uint32, bool) {
	// The stop list is small, so a linear scan would work, but a map keeps the
	// lookup flat for a network with ten thousand stops.
	index, ok := s.stopByID[stopID]
	return index, ok
}

// rawStop carries the trip through the sort, and is dropped afterwards. Every
// field is four bytes, so the transient slice stays at twenty bytes a row.
type rawStop struct {
	trip      uint32
	sequence  uint32
	stop      uint32
	arrival   int32
	departure int32
}

// readSchedule builds the timetable from stop_times.txt and frequencies.txt.
//
// The rows stream through the parser one at a time, into a flat slice that is
// sorted once. GTFS files are conventionally already sorted by trip and
// sequence, but the specification does not require it, so the sort must
// happen.
func readSchedule(reader *zip.Reader, maxRows int) (*Schedule, error) {
	if maxRows <= 0 {
		maxRows = DefaultMaxScheduleRows
	}
	schedule := &Schedule{
		tripByID:       map[string]uint32{},
		stopByID:       map[string]uint32{},
		frequencyBased: map[string]struct{}{},
	}

	raw := make([]rawStop, 0, 1<<20)
	err := rows(reader, "stop_times.txt", true, func(row map[string]string) error {
		if len(raw) >= maxRows {
			return fmt.Errorf("gtfsstatic: stop_times.txt holds more than %d rows", maxRows)
		}
		trip := row["trip_id"]
		if trip == "" {
			return nil
		}
		sequence, err := strconv.ParseUint(strings.TrimSpace(row["stop_sequence"]), 10, 32)
		if err != nil {
			// A row with no usable sequence cannot be placed in the trip.
			return nil
		}
		raw = append(raw, rawStop{
			trip:      schedule.internTrip(trip),
			sequence:  uint32(sequence),
			stop:      schedule.intern(row["stop_id"]),
			arrival:   parseGTFSTime(row["arrival_time"]),
			departure: parseGTFSTime(row["departure_time"]),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(raw, func(i, j int) bool {
		if raw[i].trip != raw[j].trip {
			return raw[i].trip < raw[j].trip
		}
		return raw[i].sequence < raw[j].sequence
	})

	schedule.rows = make([]scheduledStop, len(raw))
	schedule.spans = make([]span, len(schedule.tripByID))
	for i := range raw {
		schedule.rows[i] = scheduledStop{
			sequence:  raw[i].sequence,
			stop:      raw[i].stop,
			arrival:   raw[i].arrival,
			departure: raw[i].departure,
		}
		window := &schedule.spans[raw[i].trip]
		if i == 0 || raw[i].trip != raw[i-1].trip {
			window.start = uint32(i)
		}
		window.end = uint32(i + 1)
	}
	// Let the collector reclaim the transient slice before the next feed
	// starts loading. It is twenty bytes a row, so 44 MB for MBTA.
	raw = nil
	_ = raw

	// frequencies.txt is optional and rare. Neither Rennes nor MBTA ships one.
	err = rows(reader, "frequencies.txt", false, func(row map[string]string) error {
		if trip := row["trip_id"]; trip != "" {
			schedule.frequencyBased[trip] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return schedule, nil
}

func (s *Schedule) intern(stopID string) uint32 {
	if index, ok := s.stopByID[stopID]; ok {
		return index
	}
	index := uint32(len(s.stops))
	s.stops = append(s.stops, stopID)
	s.stopByID[stopID] = index
	return index
}

// parseGTFSTime reads a GTFS time into seconds from the origin of the service
// day.
//
// The hour field may pass 24, and it does often: 1.1% of the Rennes rows and
// 4.7% of the MBTA rows. A blank value is legal at a stop that is not a
// timepoint, and it returns noTime.
// It is hand-rolled rather than built on fmt.Sscanf, which took most of the
// load time. Parsing the 2.2 million rows of MBTA fell from 12.8 seconds to
// well under two once this function stopped reflecting over a format string.
func parseGTFSTime(value string) int32 {
	value = strings.TrimSpace(value)
	if value == "" {
		return noTime
	}
	var parts [3]int
	field, digits := 0, 0
	for i := 0; i < len(value); i++ {
		switch c := value[i]; {
		case c >= '0' && c <= '9':
			if field > 2 {
				return noTime
			}
			parts[field] = parts[field]*10 + int(c-'0')
			digits++
		case c == ':':
			if digits == 0 {
				return noTime
			}
			field++
			digits = 0
		default:
			return noTime
		}
	}
	if field != 2 || digits == 0 {
		return noTime
	}
	hours, minutes, seconds := parts[0], parts[1], parts[2]
	// The hour field passes 24 often, and legitimately: 4.7% of the MBTA rows
	// do. Only the minute and the second are bounded.
	if minutes > 59 || seconds > 59 || hours > 48 {
		return noTime
	}
	return int32(hours*3600 + minutes*60 + seconds)
}
