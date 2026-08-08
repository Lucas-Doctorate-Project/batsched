#include "green_window_filling.hpp"

#include <cmath>
#include <limits>

#include <loguru.hpp>

#include "../locality.hpp"
#include "../pempek_assert.hpp"

using namespace std;

namespace
{
const std::string CARBON_INTENSITY_PROPERTY = "carbon_intensity";
const std::string WATER_INTENSITY_PROPERTY = "water_intensity";

const double JOULES_PER_KWH = 3600000.0;

const double DEFAULT_COMPUTING_WATTS = 320.0;
const double DEFAULT_IDLE_WATTS = 10.0;
}

GreenWindowFilling::GreenWindowFilling(Workload * workload,
                                       SchedulingDecision * decision,
                                       Queue * queue,
                                       ResourceSelector * selector,
                                       double rjms_delay,
                                       rapidjson::Document * variant_options) :
    EasyBackfilling(workload, decision, queue, selector, rjms_delay, variant_options),
    _intensity_trace(get_intensity_trace_option(variant_options)),
    _intensity_zone(get_required_string_option(variant_options, "intensity_zone")),
    _csv_parser(_intensity_trace, _intensity_zone),
    _signal_property(get_signal_property_option(variant_options)),
    _planning_horizon(get_required_positive_double_option(variant_options, "planning_horizon_seconds")),
    _computing_watts(get_optional_positive_double_option(variant_options, "computing_watts",
                                                         DEFAULT_COMPUTING_WATTS)),
    _idle_watts(get_optional_positive_double_option(variant_options, "idle_watts", DEFAULT_IDLE_WATTS)),
    _green_window_filling_debug(get_optional_bool_option(variant_options, "green_window_filling_debug", false))
{
    // Candidates land on absolute multiples of this step, so they only line up
    // with the trace's intensity changes while it matches the sampling period.
    if (variant_options->HasMember("window_step_seconds"))
    {
        PPK_ASSERT_ERROR((*variant_options)["window_step_seconds"].IsNumber(),
                         "Invalid options: 'window_step_seconds' should be a number");
        double window_step = (*variant_options)["window_step_seconds"].GetDouble();
        PPK_ASSERT_ERROR(window_step > 0.0,
                         "Invalid options: 'window_step_seconds' should be strictly positive (got %g)",
                         window_step);
        _window_step = window_step;
    }
    else
    {
        double sampling_period = _csv_parser.get_sampling_period(_signal_property);

        PPK_ASSERT_ERROR(sampling_period > 0.0,
                         "Invalid options: 'window_step_seconds' is missing and no regular CSV sampling period "
                         "could be detected");
        _window_step = sampling_period;
    }

    if (_green_window_filling_debug)
    {
        LOG_F(INFO, "GreenWindowFilling initialized with intensity_trace=%s, intensity_zone=%s, "
                    "signal=%s, planning_horizon_seconds=%g, window_step_seconds=%g, "
                    "computing_watts=%g, idle_watts=%g",
              _intensity_trace.c_str(),
              _intensity_zone.c_str(),
              _signal_property.c_str(),
              (double)_planning_horizon,
              (double)_window_step,
              _computing_watts,
              _idle_watts);
    }
}

GreenWindowFilling::~GreenWindowFilling()
{
}

std::string GreenWindowFilling::get_required_string_option(rapidjson::Document * variant_options,
                                                           const char * option_name)
{
    PPK_ASSERT_ERROR(variant_options->HasMember(option_name),
                     "Invalid options: required member '%s' cannot be found", option_name);
    PPK_ASSERT_ERROR((*variant_options)[option_name].IsString(),
                     "Invalid options: '%s' should be a string", option_name);

    return (*variant_options)[option_name].GetString();
}

std::string GreenWindowFilling::get_intensity_trace_option(rapidjson::Document * variant_options)
{
    if (variant_options->HasMember("intensity_trace"))
        return get_required_string_option(variant_options, "intensity_trace");

    return get_required_string_option(variant_options, "typical_intensities_file");
}

std::string GreenWindowFilling::get_signal_property_option(rapidjson::Document * variant_options)
{
    std::string signal = get_required_string_option(variant_options, "signal");

    if (signal == "carbon")
        return CARBON_INTENSITY_PROPERTY;
    if (signal == "water")
        return WATER_INTENSITY_PROPERTY;

    PPK_ASSERT_ERROR(false,
                     "Invalid options: 'signal' should be either 'carbon' or 'water' (got '%s')",
                     signal.c_str());
    return "";
}

double GreenWindowFilling::get_required_positive_double_option(rapidjson::Document * variant_options,
                                                               const char * option_name)
{
    PPK_ASSERT_ERROR(variant_options->HasMember(option_name),
                     "Invalid options: required member '%s' cannot be found", option_name);
    PPK_ASSERT_ERROR((*variant_options)[option_name].IsNumber(),
                     "Invalid options: '%s' should be a number", option_name);

    double value = (*variant_options)[option_name].GetDouble();
    PPK_ASSERT_ERROR(value > 0.0,
                     "Invalid options: '%s' should be strictly positive (got %g)", option_name, value);
    return value;
}

double GreenWindowFilling::get_optional_positive_double_option(rapidjson::Document * variant_options,
                                                               const char * option_name,
                                                               double default_value)
{
    if (!variant_options->HasMember(option_name))
        return default_value;

    return get_required_positive_double_option(variant_options, option_name);
}

bool GreenWindowFilling::get_optional_bool_option(rapidjson::Document * variant_options,
                                                  const char * option_name,
                                                  bool default_value)
{
    if (!variant_options->HasMember(option_name))
        return default_value;

    PPK_ASSERT_ERROR((*variant_options)[option_name].IsBool(),
                     "Invalid options: '%s' should be a boolean", option_name);
    return (*variant_options)[option_name].GetBool();
}

void GreenWindowFilling::on_requested_call(double date)
{
    ISchedulingAlgorithm::on_requested_call(date);
    forget_requested_call_date(date);
}

void GreenWindowFilling::make_decisions(double date,
                                        SortableJobOrder::UpdateInformation * update_info,
                                        SortableJobOrder::CompareInformation * compare_info)
{
    Rational current_date = date;

    for (const string & ended_job_id : _jobs_ended_recently)
        _schedule.remove_job_if_exists((*_workload)[ended_job_id]);

    update_schedule_present(current_date);
    execute_reserved_job_if_ready(current_date);

    for (const string & new_job_id : _jobs_released_recently)
    {
        const Job * new_job = (*_workload)[new_job_id];

        if (new_job->nb_requested_resources > _nb_machines)
        {
            _decision->add_reject_job(new_job_id, date);
        }
        else if (!new_job->has_walltime)
        {
            LOG_SCOPE_FUNCTION(INFO);
            LOG_F(INFO, "Date=%g. Rejecting job '%s' as it has no walltime", date, new_job_id.c_str());
            _decision->add_reject_job(new_job_id, date);
        }
        else
        {
            _queue->append_job(new_job, update_info);
        }
    }

    _queue->sort_queue(update_info, compare_info);
    schedule_priority_job(current_date);
    request_reservation_call(current_date);
}

void GreenWindowFilling::update_schedule_present(Rational date)
{
    PPK_ASSERT_ERROR(_schedule.nb_slices() > 0);

    if (date < _schedule.begin()->end)
        _schedule.update_first_slice(date);
    else
        _schedule.update_first_slice_removing_remaining_jobs(date);
}

void GreenWindowFilling::execute_reserved_job_if_ready(Rational date)
{
    if (!has_pending_displacement() || date < _reserved_start)
        return;

    if (_green_window_filling_debug)
    {
        LOG_F(INFO, "GreenWindowFilling executing displaced job '%s' at date=%g on machines=%s",
              _reserved_job->id.c_str(),
              (double)date,
              _reserved_machines.to_string_hyphen(" ", "-").c_str());
    }

    start_job(_reserved_job, _reserved_machines, date);
    clear_reservation();
}

void GreenWindowFilling::schedule_priority_job(Rational date)
{
    if (has_pending_displacement())
        return;

    const Job * job = priority_job_ready_to_start(date);
    if (job == nullptr)
        return;

    // t0 is always among the candidates, and it is already known to fit (that
    // is what priority_job_ready_to_start just checked), so a window is always
    // found.
    WindowCandidate candidate = find_best_window(job, date);
    PPK_ASSERT_ERROR(candidate.found, "No feasible window found for job '%s' despite fitting at t0=%g",
                     job->id.c_str(), (double)date);

    Schedule::JobAlloc alloc = insert_at_scored_window(job, candidate);

    PPK_ASSERT_ERROR(alloc.has_been_inserted);
    PPK_ASSERT_ERROR(alloc.begin >= date,
                     "Invalid allocation for job '%s': begin=%g, current date=%g",
                     job->id.c_str(), (double)alloc.begin, (double)date);

    if (alloc.begin == date)
    {
        if (_green_window_filling_debug)
        {
            LOG_F(INFO, "GreenWindowFilling immediately executing job '%s' at date=%g on machines=%s",
                  job->id.c_str(), (double)date, alloc.used_machines.to_string_hyphen(" ", "-").c_str());
        }

        start_job(job, alloc.used_machines, date);
    } else
        hold_displaced_reservation(job, alloc);
}

// Only the head of the FCFS queue is ever displaced, and only one displacement
// is in flight at a time. While a reservation is pending the platform is held:
// starting anything else would fill the very idle gap that the displacement
// created, cancelling out the reason to displace.
bool GreenWindowFilling::has_pending_displacement() const
{
    return _reserved_job != nullptr;
}

// The queue head, but only once it could actually start: t0 is the date at
// which it fits. Until then there is nothing to displace, so the caller waits
// for a running job to end.
const Job * GreenWindowFilling::priority_job_ready_to_start(Rational date) const
{
    const Job * job = _queue->first_job_or_nullptr();
    if (job == nullptr)
        return nullptr;

    IntervalSet machines_now;
    if (!find_exact_allocation(job, date, date + job->walltime, machines_now))
        return nullptr;

    return job;
}

Schedule::JobAlloc GreenWindowFilling::insert_at_scored_window(const Job * job, const WindowCandidate & candidate)
{
    LimitedRangeResourceSelector exact_selector(candidate.machines);
    Schedule::JobAlloc alloc = _schedule.add_job_first_fit_after_time(job, candidate.begin, &exact_selector);

    PPK_ASSERT_ERROR(alloc.begin == candidate.begin,
                     "Job '%s' was expected to start exactly at %g, but starts at %g",
                     job->id.c_str(), (double)candidate.begin, (double)alloc.begin);
    PPK_ASSERT_ERROR(alloc.used_machines == candidate.machines,
                     "Job '%s' was expected to use machines %s, but uses %s",
                     job->id.c_str(),
                     candidate.machines.to_string_brackets().c_str(),
                     alloc.used_machines.to_string_brackets().c_str());

    if (_green_window_filling_debug)
    {
        LOG_F(INFO, "GreenWindowFilling picked window [%g,%g) for job '%s', impact=%g, machines=%s",
              (double)candidate.begin,
              (double)candidate.end,
              job->id.c_str(),
              candidate.score,
              candidate.machines.to_string_hyphen(" ", "-").c_str());
    }

    return alloc;
}

void GreenWindowFilling::start_job(const Job * job, const IntervalSet & machines, Rational date)
{
    _decision->add_execute_job(job->id, machines, (double)date);
    _queue->remove_job(job);
}

void GreenWindowFilling::hold_displaced_reservation(const Job * job, const Schedule::JobAlloc & alloc)
{
    _reserved_job = job;
    _reserved_start = alloc.begin;
    _reserved_machines = alloc.used_machines;
}

void GreenWindowFilling::clear_reservation()
{
    _reserved_job = nullptr;
    _reserved_start = 0;
    _reserved_machines = IntervalSet::empty_interval_set();
}

GreenWindowFilling::WindowCandidate GreenWindowFilling::find_best_window(const Job * job, Rational date) const
{
    WindowCandidate best;
    Rational horizon_end = date + _planning_horizon;

    // t0 is always a candidate, even though it seldom sits on the trace grid.
    consider_window(job, date, date, best);

    // Then every later grid point within the maximum displacement. dm bounds
    // where the job may start, not where it must finish, so its window can
    // run past horizon_end.
    for (Rational begin = first_grid_point_after(date);
         begin <= horizon_end;
         begin += _window_step)
    {
        consider_window(job, date, begin, best);
    }

    return best;
}

Rational GreenWindowFilling::first_grid_point_after(Rational date) const
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

void GreenWindowFilling::consider_window(const Job * job, Rational t0, Rational begin, WindowCandidate & best) const
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

bool GreenWindowFilling::find_exact_allocation(const Job * job, Rational begin, Rational end, IntervalSet & machines) const
{
    IntervalSet available_machines = available_machines_during_period(begin, end);
    return _selector->fit(job, available_machines, machines);
}

IntervalSet GreenWindowFilling::available_machines_during_period(Rational begin, Rational end) const
{
    PPK_ASSERT_ERROR(begin < end);

    IntervalSet available_machines;
    bool initialized = false;

    for (auto slice_it = _schedule.begin(); slice_it != _schedule.end(); ++slice_it)
    {
        if (slice_it->end <= begin)
            continue;
        if (slice_it->begin >= end)
            break;

        if (!initialized)
        {
            available_machines = slice_it->available_machines;
            initialized = true;
        }
        else
            available_machines &= slice_it->available_machines;
    }

    if (!initialized)
        return IntervalSet::empty_interval_set();

    return available_machines;
}

// The environmental impact of holding the job's nodes from t0 to the end of its
// run: they idle from t0 until begin, then compute until end. Returned in the
// signal's own unit (gCO2eq for carbon, litres for water).
double GreenWindowFilling::compute_window_score(const Job * job, Rational t0, Rational begin, Rational end) const
{
    double computing_impact = _computing_watts * intensity_sum(begin, end);
    double idle_impact = (begin > t0) ? _idle_watts * intensity_sum(t0, begin) : 0.0;

    return (double)job->nb_requested_resources * (computing_impact + idle_impact) / JOULES_PER_KWH;
}

double GreenWindowFilling::intensity_sum(Rational begin, Rational end) const
{
    double sum = _csv_parser.get_sum(_signal_property, (double)begin, (double)end);

    PPK_ASSERT_ERROR(!std::isnan(sum),
                     "Intensity trace '%s' has no '%s' data for zone '%s' over [%g,%g)",
                     _intensity_trace.c_str(),
                     _signal_property.c_str(),
                     _intensity_zone.c_str(),
                     (double)begin,
                     (double)end);

    return sum;
}

void GreenWindowFilling::request_reservation_call(Rational date)
{
    if (_reserved_job == nullptr || _reserved_start <= date)
        return;

    double future_date = (double)_reserved_start;
    if (is_call_date_already_requested(future_date))
        return;

    if (_green_window_filling_debug)
    {
        LOG_F(INFO, "GreenWindowFilling requesting callback at date=%g", future_date);
    }

    _decision->add_call_me_later(future_date, (double)date);
    _requested_call_dates.insert(future_date);
}

void GreenWindowFilling::forget_requested_call_date(double date)
{
    for (auto it = _requested_call_dates.begin(); it != _requested_call_dates.end(); )
    {
        if (*it <= date + 1e-9)
            it = _requested_call_dates.erase(it);
        else
            ++it;
    }
}

bool GreenWindowFilling::is_call_date_already_requested(double date) const
{
    for (double requested_date : _requested_call_dates)
        if (same_date(requested_date, date))
            return true;

    return false;
}

bool GreenWindowFilling::same_date(double left, double right)
{
    return std::abs(left - right) <= 1e-9;
}
