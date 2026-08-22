package collector

import (
	"math"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"

	"github.com/raspbeguy/gtfs_exporter/internal/gtfsstatic"
)

// noScheduleReason names why a trip could not be timed against the timetable.
// It becomes the reason label of gtfs_rt_trips_without_schedule.
type noScheduleReason string

const (
	reasonNotInDataset   noScheduleReason = "not_in_dataset"
	reasonNoStopTimes    noScheduleReason = "no_stop_times"
	reasonFrequencyBased noScheduleReason = "frequency_based"
	reasonIndexNotLoaded noScheduleReason = "index_not_loaded"
	reasonDatasetExpired noScheduleReason = "dataset_expired"
	reasonNoTimezone     noScheduleReason = "no_timezone"
	reasonNoMatch        noScheduleReason = "no_matching_stop"
	// reasonNoTimeableStop is common and healthy, not a fault, and it does not
	// mean that the trip is cancelled.
	//
	// MBTA sends about 40% of its trip updates as a delta that skips a stop or
	// two on a trip which is otherwise running: measured, those updates carry
	// a median of 2 stops against 22 for a normal one. Every stop they carry
	// is SKIPPED, so none can be timed, but the trip runs.
	reasonNoTimeableStop noScheduleReason = "no_timeable_stop"
)

// halfDay is the window inside which a predicted time is taken to belong to
// the candidate service day. Beyond it, the trip crossed midnight and the day
// shifts.
const halfDay = 12 * time.Hour

// scheduleDelay computes the delay of a trip against the static timetable.
//
// This is the source of last resort, and the only one that works for a
// producer which publishes absolute times and no delay. Rennes and MBTA both
// do, which is 2 of the 6 trip-update feeds measured.
//
// It returns the delay in seconds, or a reason why it could not.
func scheduleDelay(update *gtfs.TripUpdate, now time.Time, index *gtfsstatic.Index) (float64, noScheduleReason, bool) {
	schedule := index.Schedule()
	if schedule == nil {
		return 0, reasonIndexNotLoaded, false
	}
	// A timetable outside its validity window describes a service that no
	// longer runs. A delay measured against it is wrong rather than missing,
	// which is worse, so refuse. Rennes publishes a twelve day window.
	if !index.InService(now) {
		return 0, reasonDatasetExpired, false
	}

	trip := update.GetTrip()
	if trip == nil || trip.TripId == nil {
		return 0, reasonNotInDataset, false
	}
	tripID := trip.GetTripId()

	// The stop times of a frequency-based trip count from the start of the
	// trip and not from the service day, so reading them as absolute would
	// report a delay of hours.
	if schedule.FrequencyBased(tripID) {
		return 0, reasonFrequencyBased, false
	}
	route, known := index.RouteOfTrip(tripID)
	if !known {
		return 0, reasonNotInDataset, false
	}
	if !schedule.Has(tripID) {
		return 0, reasonNoStopTimes, false
	}
	loc, found := index.Location(route)
	if !found {
		// Guessing a zone would put the answer hours out. Refuse instead.
		return 0, reasonNoTimezone, false
	}

	origins := serviceDayOrigins(trip, now, loc)
	// fallback holds the last stop that the vehicle has already passed. A feed
	// carries trips that are nearly finished, and every one of their
	// predictions lies in the past. Without this, MBTA lost 745 of about 1600
	// trips. The stated-delay path above keeps the same fallback, so the two
	// sources behave alike.
	fallback, hasFallback := 0.0, false
	// usable counts the stops that carry a prediction at all, so that a trip
	// which is nothing but skip notices is reported as such rather than as a
	// failed join.
	usable := 0

	for _, stop := range update.GetStopTimeUpdate() {
		switch stop.GetScheduleRelationship() {
		case gtfs.TripUpdate_StopTimeUpdate_SKIPPED, gtfs.TripUpdate_StopTimeUpdate_NO_DATA:
			continue
		}
		usable++
		// Compare like with like. A predicted departure against a scheduled
		// arrival would add the dwell time to the delay. Dwell is zero on
		// 99.9% of the Rennes rows, but on those that dwell the median is 300
		// seconds and the longest is 1740.
		for _, side := range []struct {
			event     *gtfs.TripUpdate_StopTimeEvent
			departure bool
		}{
			{stop.GetDeparture(), true},
			{stop.GetArrival(), false},
		} {
			if side.event == nil || side.event.Time == nil {
				continue
			}
			predicted := side.event.GetTime()
			scheduled, ok := scheduledSeconds(schedule, tripID, stop, side.departure)
			if !ok {
				continue
			}
			delay, ok := matchServiceDay(predicted, scheduled, origins)
			if !ok {
				continue
			}
			// The first stop that the vehicle has not passed is the one that
			// describes the trip now.
			if predicted >= now.Unix() {
				return delay, "", true
			}
			fallback, hasFallback = delay, true
		}
	}
	if hasFallback {
		return fallback, "", true
	}
	if usable == 0 {
		return 0, reasonNoTimeableStop, false
	}
	return 0, reasonNoMatch, false
}

// scheduledSeconds finds the scheduled time of one stop time update, in
// seconds from the origin of the service day.
//
// It joins on stop_sequence when the producer sends one, as MBTA does on all
// of its stop time updates, and on stop_id otherwise, as Rennes forces.
func scheduledSeconds(schedule *gtfsstatic.Schedule, tripID string,
	stop *gtfs.TripUpdate_StopTimeUpdate, departure bool,
) (int32, bool) {
	pick := func(arrival, dep int32) (int32, bool) {
		value := arrival
		if departure {
			value = dep
		}
		return value, value >= 0
	}

	if stop.StopSequence != nil {
		arrival, dep, ok := schedule.BySequence(tripID, stop.GetStopSequence())
		if !ok {
			return 0, false
		}
		return pick(arrival, dep)
	}
	if stop.StopId == nil {
		return 0, false
	}
	// A loop route calls at the same stop twice, so this returns every call.
	// MBTA does it on 221 rows across 209 trips.
	calls := schedule.ByStop(tripID, stop.GetStopId())
	if len(calls) != 1 {
		// With no sequence to disambiguate a repeated stop, any choice could
		// be the wrong one. Emit nothing rather than a wrong number.
		return 0, false
	}
	return pick(calls[0].Arrival, calls[0].Departure)
}

// serviceDayOrigins returns the candidate origins of the service day, most
// likely first.
//
// The feed states the day when it can: MBTA sets start_date on all 1,657 of
// its trips. Rennes sets none, so the day is inferred from the clock and the
// neighbours are offered as candidates, because a trip that runs at 00:30
// belongs to the day before.
func serviceDayOrigins(trip *gtfs.TripDescriptor, now time.Time, loc *time.Location) []int64 {
	if trip.StartDate != nil {
		if day, err := time.ParseInLocation("20060102", trip.GetStartDate(), loc); err == nil {
			return []int64{gtfsstatic.ServiceDayOrigin(day, loc).Unix()}
		}
	}
	local := now.In(loc)
	return []int64{
		gtfsstatic.ServiceDayOrigin(local, loc).Unix(),
		gtfsstatic.ServiceDayOrigin(local.AddDate(0, 0, -1), loc).Unix(),
		gtfsstatic.ServiceDayOrigin(local.AddDate(0, 0, 1), loc).Unix(),
	}
}

// matchServiceDay picks the candidate day that places the scheduled time
// nearest the predicted one, and returns the difference.
//
// A candidate more than twelve hours out belongs to another day, so it is
// rejected rather than reported as a twenty hour delay.
func matchServiceDay(predicted int64, scheduled int32, origins []int64) (float64, bool) {
	best, found := math.Inf(1), false
	for _, origin := range origins {
		difference := float64(predicted - (origin + int64(scheduled)))
		if math.Abs(difference) >= halfDay.Seconds() {
			continue
		}
		if !found || math.Abs(difference) < math.Abs(best) {
			best, found = difference, true
		}
	}
	return best, found
}
