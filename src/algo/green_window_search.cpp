#include "green_window_scheduling.hpp"

#include <cmath>

#include "../pempek_assert.hpp"

namespace
{
const double JOULES_PER_KWH = 3600000.0;
}

GreenWindowScheduling::WindowCandidate GreenWindowScheduling::find_best_window(const Job *job, Rational date)
{
    WindowCandidate best;
    const Rational t0 = _displacement_origins.emplace(job, date).first->second;
    const Rational horizon_end = t0 + _planning_horizon;
    // The current runnable date remains a candidate off-grid and at an expired deadline.
    // b46671e renewed the horizon here, allowing successive searches to drift.
    consider_window(job, t0, date, best);
    // Then every later grid point within the maximum displacement. dm bounds
    // where the job may start, not where it must finish, so its window can
    // run past horizon_end.
    for (Rational begin = first_grid_point_after(date); begin <= horizon_end; begin += _window_step)
    {
        consider_window(job, t0, begin, best);
    }
    log_window_choice(job, best);
    return best;
}

Rational GreenWindowScheduling::first_grid_point_after(Rational date) const
{
    PPK_ASSERT_ERROR(date >= 0, "Negative date %g", (double)date);

    // Truncating the quotient is a floor here, since the date is non-negative.
    Rational quotient = date / _window_step;
    boost::multiprecision::mpz_int index = numerator(quotient) / denominator(quotient);
    Rational grid_point = Rational(index) * _window_step;

    if (grid_point <= date)
        grid_point += _window_step;

    return grid_point;
}

void GreenWindowScheduling::consider_window(const Job *job, Rational t0, Rational begin, WindowCandidate &best) const
{
    Rational end = begin + job->walltime;
    IntervalSet machines;

    if (!find_exact_allocation(job, begin, end, machines))
        return;

    // On a tie the window already picked wins. Candidates are visited in
    // increasing start date, so that is the earliest one.
    double score = compute_window_score(job, t0, begin, end);
    if (best.found && score >= best.score - 1e-9)
        return;

    best.found = true;
    best.begin = begin;
    best.end = end;
    best.score = score;
    best.machines = machines;
}

bool GreenWindowScheduling::find_exact_allocation(
    const Job *job, Rational begin, Rational end, IntervalSet &machines) const
{
    IntervalSet available_machines = available_machines_during_period(begin, end);
    // Keep only the displaced allocation idle, including jobs that finish before its start.
    if (has_pending_displacement() && begin < _reserved_start)
        available_machines -= _reserved_machines;
    return _selector->fit(job, available_machines, machines);
}

IntervalSet GreenWindowScheduling::available_machines_during_period(Rational begin, Rational end) const
{
    PPK_ASSERT_ERROR(begin < end, "Invalid interval [%g,%g), expected begin < end", (double)begin, (double)end);
    IntervalSet available_machines = _schedule.begin()->available_machines;
    bool initialized = false;
    for (const Schedule::TimeSlice &slice : _schedule)
    {
        if (slice.end <= begin)
            continue;
        if (slice.begin >= end)
            break;
        available_machines = initialized ? available_machines & slice.available_machines : slice.available_machines;
        initialized = true;
    }
    return initialized ? available_machines : IntervalSet::empty_interval_set();
}

// The environmental impact of holding the job's nodes from t0 to the end of its
// run: they idle from t0 until begin, then compute until end. Returned in the
// signal's own unit (gCO2eq for carbon, litres for water).
double GreenWindowScheduling::compute_window_score(const Job *job, Rational t0, Rational begin, Rational end) const
{
    double computing_impact = _computing_watts * intensity_sum(begin, end);
    double idle_impact = (begin > t0) ? _idle_watts * intensity_sum(t0, begin) : 0.0;

    return (double)job->nb_requested_resources * (computing_impact + idle_impact) / JOULES_PER_KWH;
}

double GreenWindowScheduling::intensity_sum(Rational begin, Rational end) const
{
    double sum = _intensity->integral(_signal_property, (double)begin, (double)end);

    PPK_ASSERT_ERROR(!std::isnan(sum), "Intensity trace '%s' has no '%s' data for zone '%s' over [%g,%g)",
        _intensity_trace.c_str(), _signal_property.c_str(), _intensity_zone.c_str(), (double)begin, (double)end);

    return sum;
}
