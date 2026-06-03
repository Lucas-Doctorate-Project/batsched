#include "greenfilling.hpp"

#include <cmath>

#include <loguru.hpp>

#include "../pempek_assert.hpp"

using namespace std;

Greenfilling::Greenfilling(Workload * workload,
                           SchedulingDecision * decision,
                           Queue * queue,
                           ResourceSelector * selector,
                           double rjms_delay,
                           rapidjson::Document * variant_options) :
    EasyBackfilling(workload, decision, queue, selector, rjms_delay, variant_options)
{
    PPK_ASSERT_ERROR(variant_options->HasMember("intensity_trace"),
                     "Greenfilling: required option 'intensity_trace' (CSV path) is missing");
    PPK_ASSERT_ERROR((*variant_options)["intensity_trace"].IsString(),
                     "Greenfilling: option 'intensity_trace' must be a string");
    string intensity_trace = (*variant_options)["intensity_trace"].GetString();

    PPK_ASSERT_ERROR(variant_options->HasMember("intensity_zone"),
                     "Greenfilling: required option 'intensity_zone' is missing");
    PPK_ASSERT_ERROR((*variant_options)["intensity_zone"].IsString(),
                     "Greenfilling: option 'intensity_zone' must be a string");
    string intensity_zone = (*variant_options)["intensity_zone"].GetString();

    _intensity_source = std::make_unique<CSV_Parser>(intensity_trace, intensity_zone);

    if (variant_options->HasMember("smoothing_factor"))
        _smoothing_factor = (*variant_options)["smoothing_factor"].GetDouble();

    if (variant_options->HasMember("ema_threshold"))
        _ema_threshold = (*variant_options)["ema_threshold"].GetDouble();

    if (variant_options->HasMember("backfilling_combinator"))
    {
        PPK_ASSERT_ERROR((*variant_options)["backfilling_combinator"].IsString(),
                         "Greenfilling: option 'backfilling_combinator' must be a string ('and' or 'or')");
        string combinator_str = (*variant_options)["backfilling_combinator"].GetString();
        if (combinator_str == "and")
            _combinator = Combinator::And;
        else if (combinator_str == "or")
            _combinator = Combinator::Or;
        else
            PPK_ASSERT_ERROR(false,
                             "Greenfilling: unknown 'backfilling_combinator' value '%s' (expected 'and' or 'or')",
                             combinator_str.c_str());
    }

    if (variant_options->HasMember("greenfilling_debug"))
        _greenfilling_debug = (*variant_options)["greenfilling_debug"].GetBool();

    if (_greenfilling_debug)
        LOG_F(INFO, "Greenfilling initialized with intensity_trace='%s', intensity_zone='%s', "
                    "smoothing_factor=%g, ema_threshold=%g, combinator=%s",
              intensity_trace.c_str(), intensity_zone.c_str(), _smoothing_factor, _ema_threshold,
              _combinator == Combinator::And ? "and" : "or");
}

Greenfilling::~Greenfilling()
{
}

void Greenfilling::on_simulation_start(double date, const rapidjson::Value & batsim_config)
{
    EasyBackfilling::on_simulation_start(date, batsim_config);
    sample_intensities(date);
}

void Greenfilling::update_ema(double intensity, double & ema, bool & initialized, const char * label)
{
    if (!initialized)
    {
        ema = intensity;
        initialized = true;
        if (_greenfilling_debug)
            LOG_F(INFO, "%s EMA initialized to %g", label, ema);
    }
    else
    {
        ema = _smoothing_factor * intensity + (1.0 - _smoothing_factor) * ema;
        if (_greenfilling_debug)
            LOG_F(INFO, "%s EMA updated to %g (current=%g)", label, ema, intensity);
    }
}

void Greenfilling::sample_intensities(double date)
{
    double carbon = _intensity_source->get_value(date, "carbon_intensity");
    double water = _intensity_source->get_value(date, "water_intensity");

    if (!std::isnan(carbon))
    {
        _carbon_intensity = carbon;
        update_ema(carbon, _carbon_ema, _carbon_ema_initialized, "Carbon");
    }
    if (!std::isnan(water))
    {
        _water_intensity = water;
        update_ema(water, _water_ema, _water_ema_initialized, "Water");
    }
}

bool Greenfilling::should_allow_backfilling() const
{
    if (!_carbon_ema_initialized && !_water_ema_initialized)
        return true;

    if (_carbon_ema_initialized && !_water_ema_initialized)
        return _carbon_intensity <= _ema_threshold * _carbon_ema;

    if (!_carbon_ema_initialized && _water_ema_initialized)
        return _water_intensity <= _ema_threshold * _water_ema;

    const bool carbon_ok = _carbon_intensity <= _ema_threshold * _carbon_ema;
    const bool water_ok = _water_intensity <= _ema_threshold * _water_ema;
    return _combinator == Combinator::And ? (carbon_ok && water_ok)
                                          : (carbon_ok || water_ok);
}

void Greenfilling::make_decisions(double date,
                                  SortableJobOrder::UpdateInformation * update_info,
                                  SortableJobOrder::CompareInformation * compare_info)
{
    sample_intensities(date);

    const Job * priority_job_before = _queue->first_job_or_nullptr();

    // Remove finished jobs from the schedule
    for (const string & ended_job_id : _jobs_ended_recently)
        _schedule.remove_job((*_workload)[ended_job_id]);

    // Handle recently released jobs
    std::vector<std::string> recently_queued_jobs;
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
            recently_queued_jobs.push_back(new_job_id);
        }
    }

    // Update the schedule's present
    _schedule.update_first_slice(date);

    // Queue sorting and priority job handling
    const Job * priority_job_after = nullptr;
    sort_queue_while_handling_priority_job(priority_job_before, priority_job_after, update_info, compare_info);

    bool allow_backfilling = should_allow_backfilling();

    if (_greenfilling_debug)
    {
        LOG_F(INFO, "Greenfilling decision at date=%g: allow_backfilling=%d "
                    "(ema_threshold=%g, combinator=%s)",
              date, (int)allow_backfilling, _ema_threshold,
              _combinator == Combinator::And ? "and" : "or");
        LOG_F(INFO, "  Carbon: current=%g, ema=%g, threshold=%g, initialized=%d",
              _carbon_intensity, _carbon_ema, _ema_threshold * _carbon_ema,
              (int)_carbon_ema_initialized);
        LOG_F(INFO, "  Water: current=%g, ema=%g, threshold=%g, initialized=%d",
              _water_intensity, _water_ema, _ema_threshold * _water_ema,
              (int)_water_ema_initialized);
    }

    // If no resources have been released, try to backfill newly-queued jobs (if allowed)
    if (_jobs_ended_recently.empty())
    {
        if (allow_backfilling)
        {
            int nb_available_machines = _schedule.begin()->available_machines.size();

            for (unsigned int i = 0; i < recently_queued_jobs.size() && nb_available_machines > 0; ++i)
            {
                const string & new_job_id = recently_queued_jobs[i];
                const Job * new_job = (*_workload)[new_job_id];

                if (_queue->contains_job(new_job) &&
                    new_job != priority_job_after &&
                    new_job->nb_requested_resources <= nb_available_machines)
                {
                    Schedule::JobAlloc alloc = _schedule.add_job_first_fit(new_job, _selector);
                    if (alloc.started_in_first_slice)
                    {
                        _decision->add_execute_job(new_job_id, alloc.used_machines, date);
                        _queue->remove_job(new_job);
                        nb_available_machines -= new_job->nb_requested_resources;
                    }
                    else
                        _schedule.remove_job(new_job);
                }
            }
        }
    }
    else
    {
        // Some resources have been released; traverse the whole queue.
        auto job_it = _queue->begin();
        int nb_available_machines = _schedule.begin()->available_machines.size();

        while (job_it != _queue->end() && nb_available_machines > 0)
        {
            const Job * job = (*job_it)->job;

            if (_schedule.contains_job(job))
                _schedule.remove_job(job);

            if (job == priority_job_after) // Priority job: always schedule
            {
                Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job, _selector);

                if (alloc.started_in_first_slice)
                {
                    _decision->add_execute_job(job->id, alloc.used_machines, date);
                    job_it = _queue->remove_job(job_it);
                    priority_job_after = _queue->first_job_or_nullptr();
                }
                else
                    ++job_it;
            }
            else if (allow_backfilling) // Non-priority job: only if backfilling allowed
            {
                Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job, _selector);

                if (alloc.started_in_first_slice)
                {
                    _decision->add_execute_job(job->id, alloc.used_machines, date);
                    job_it = _queue->remove_job(job_it);
                }
                else
                {
                    _schedule.remove_job(job);
                    ++job_it;
                }
            }
            else // Backfilling blocked: skip non-priority job
            {
                ++job_it;
            }
        }
    }
}
