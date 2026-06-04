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
    _planning_horizon(get_required_positive_double_option(variant_options, "planning_horizon_seconds")),
    _carbon_weight(get_optional_nonnegative_double_option(variant_options, "carbon_weight", 1.0)),
    _water_weight(get_optional_nonnegative_double_option(variant_options, "water_weight", 1.0)),
    _green_window_filling_debug(get_optional_bool_option(variant_options, "green_window_filling_debug", false))
{
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
        double sampling_period = _csv_parser.get_sampling_period(CARBON_INTENSITY_PROPERTY);
        if (std::isnan(sampling_period))
            sampling_period = _csv_parser.get_sampling_period(WATER_INTENSITY_PROPERTY);

        PPK_ASSERT_ERROR(sampling_period > 0.0,
                         "Invalid options: 'window_step_seconds' is missing and no regular CSV sampling period "
                         "could be detected");
        _window_step = sampling_period;
    }

    if (_green_window_filling_debug)
    {
        LOG_F(INFO, "GreenWindowFilling initialized with intensity_trace=%s, intensity_zone=%s, "
                    "planning_horizon_seconds=%g, window_step_seconds=%g, "
                    "carbon_weight=%g, water_weight=%g",
              _intensity_trace.c_str(),
              _intensity_zone.c_str(),
              (double)_planning_horizon,
              (double)_window_step,
              _carbon_weight,
              _water_weight);
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

double GreenWindowFilling::get_optional_nonnegative_double_option(rapidjson::Document * variant_options,
                                                                  const char * option_name,
                                                                  double default_value)
{
    if (!variant_options->HasMember(option_name))
        return default_value;

    PPK_ASSERT_ERROR((*variant_options)[option_name].IsNumber(),
                     "Invalid options: '%s' should be a number", option_name);
    double value = (*variant_options)[option_name].GetDouble();
    PPK_ASSERT_ERROR(value >= 0.0,
                     "Invalid options: '%s' should be non-negative (got %g)", option_name, value);
    return value;
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
    execute_ready_jobs(date);

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
    reserve_unscheduled_jobs(current_date);
    request_earliest_future_reservation_call(current_date);
}

void GreenWindowFilling::update_schedule_present(Rational date)
{
    PPK_ASSERT_ERROR(_schedule.nb_slices() > 0);

    if (date < _schedule.begin()->end)
        _schedule.update_first_slice(date);
    else
        _schedule.update_first_slice_removing_remaining_jobs(date);
}

void GreenWindowFilling::execute_ready_jobs(double date)
{
    vector<const Job *> jobs_to_remove;

    for (auto allocated_job : _schedule.begin()->allocated_jobs)
    {
        const Job * job = allocated_job.first;
        const IntervalSet & used_machines = allocated_job.second;

        if (_queue->contains_job(job))
        {
            if (_green_window_filling_debug)
            {
                LOG_F(INFO, "GreenWindowFilling executing reserved job '%s' at date=%g on machines=%s",
                      job->id.c_str(), date, used_machines.to_string_hyphen(" ", "-").c_str());
            }

            _decision->add_execute_job(job->id, used_machines, date);
            jobs_to_remove.push_back(job);
        }
    }

    for (const Job * job : jobs_to_remove)
        _queue->remove_job(job);
}

void GreenWindowFilling::reserve_unscheduled_jobs(Rational date)
{
    for (auto job_it = _queue->begin(); job_it != _queue->end(); )
    {
        const Job * job = (*job_it)->job;

        if (_schedule.contains_job(job))
        {
            ++job_it;
            continue;
        }

        Schedule::JobAlloc alloc = reserve_job(job, date);
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

            _decision->add_execute_job(job->id, alloc.used_machines, (double)date);
            job_it = _queue->remove_job(job_it);
        }
        else
        {
            ++job_it;
        }
    }
}

Schedule::JobAlloc GreenWindowFilling::reserve_job(const Job * job, Rational date)
{
    WindowCandidate candidate = find_best_window(job, date);

    if (candidate.found)
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
            LOG_F(INFO, "GreenWindowFilling reserved job '%s' in scored window [%g,%g), score=%g, machines=%s",
                  job->id.c_str(),
                  (double)candidate.begin,
                  (double)candidate.end,
                  candidate.score,
                  candidate.machines.to_string_hyphen(" ", "-").c_str());
        }

        return alloc;
    }

    Rational fallback_start = date + _planning_horizon;
    Schedule::JobAlloc alloc = _schedule.add_job_first_fit_after_time(job, fallback_start, _selector);

    if (_green_window_filling_debug)
    {
        LOG_F(INFO, "GreenWindowFilling found no feasible scored window for job '%s'; "
                    "reserved first-fit allocation [%g,%g), machines=%s",
              job->id.c_str(),
              (double)alloc.begin,
              (double)alloc.end,
              alloc.used_machines.to_string_hyphen(" ", "-").c_str());
    }

    return alloc;
}

GreenWindowFilling::WindowCandidate GreenWindowFilling::find_best_window(const Job * job, Rational date) const
{
    WindowCandidate best;
    Rational horizon_end = date + _planning_horizon;

    if (date + job->walltime > horizon_end)
        return best;

    for (Rational begin = date; begin + job->walltime <= horizon_end; begin += _window_step)
    {
        Rational end = begin + job->walltime;
        IntervalSet machines;

        if (!find_exact_allocation(job, begin, end, machines))
            continue;

        double score = compute_window_score(job, begin, end);
        if (!best.found || score < best.score - 1e-9)
        {
            best.found = true;
            best.begin = begin;
            best.end = end;
            best.score = score;
            best.machines = machines;
        }
    }

    return best;
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

double GreenWindowFilling::compute_window_score(const Job * job, Rational begin, Rational end) const
{
    double begin_date = (double)begin;
    double end_date = (double)end;
    double duration = end_date - begin_date;
    double carbon_sum = _csv_parser.get_sum(CARBON_INTENSITY_PROPERTY, begin_date, end_date);
    double water_sum = _csv_parser.get_sum(WATER_INTENSITY_PROPERTY, begin_date, end_date);

    double normalized_carbon_sum = normalize_intensity_sum(carbon_sum,
                                                           _csv_parser.get_min(CARBON_INTENSITY_PROPERTY),
                                                           _csv_parser.get_max(CARBON_INTENSITY_PROPERTY),
                                                           duration);
    double normalized_water_sum = normalize_intensity_sum(water_sum,
                                                          _csv_parser.get_min(WATER_INTENSITY_PROPERTY),
                                                          _csv_parser.get_max(WATER_INTENSITY_PROPERTY),
                                                          duration);

    return (double)job->nb_requested_resources *
           ((_carbon_weight * normalized_carbon_sum) + (_water_weight * normalized_water_sum));
}

double GreenWindowFilling::normalize_intensity_sum(double intensity_sum,
                                                   double min_intensity,
                                                   double max_intensity,
                                                   double duration) const
{
    if (std::isnan(intensity_sum) || std::isnan(min_intensity) || std::isnan(max_intensity))
        return 0.0;

    if (max_intensity <= min_intensity)
        return 0.0;

    double normalized_sum = (intensity_sum - (min_intensity * duration)) /
                            (max_intensity - min_intensity);

    if (normalized_sum < 0.0 && normalized_sum > -1e-9)
        return 0.0;

    return normalized_sum;
}

void GreenWindowFilling::request_earliest_future_reservation_call(Rational date)
{
    bool found = false;
    Rational earliest_date = 0;

    for (auto slice_it = _schedule.begin(); slice_it != _schedule.end() && !found; ++slice_it)
    {
        if (slice_it->begin <= date)
            continue;

        for (auto allocated_job : slice_it->allocated_jobs)
        {
            const Job * job = allocated_job.first;

            if (_queue->contains_job(job))
            {
                earliest_date = slice_it->begin;
                found = true;
                break;
            }
        }
    }

    if (!found)
        return;

    double future_date = (double)earliest_date;
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
