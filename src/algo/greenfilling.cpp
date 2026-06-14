#include "greenfilling.hpp"

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

    if (variant_options->HasMember("signal"))
    {
        PPK_ASSERT_ERROR((*variant_options)["signal"].IsString(),
                         "Greenfilling: option 'signal' must be a string ('carbon' or 'water')");
        string signal_str = (*variant_options)["signal"].GetString();
        if (signal_str == "carbon")
            _signal = Signal::Carbon;
        else if (signal_str == "water")
            _signal = Signal::Water;
        else
            PPK_ASSERT_ERROR(false, "Greenfilling: unknown 'signal' value '%s' (expected 'carbon' or 'water')", signal_str.c_str());
    }

    if (variant_options->HasMember("smoothing_factor"))
        _smoothing_factor = (*variant_options)["smoothing_factor"].GetDouble();

    if (variant_options->HasMember("greenfilling_debug"))
        _greenfilling_debug = (*variant_options)["greenfilling_debug"].GetBool();

    if (_greenfilling_debug)
        LOG_F(INFO, "Greenfilling initialized with intensity_trace='%s', intensity_zone='%s', signal=%s, smoothing_factor=%g",
              intensity_trace.c_str(), intensity_zone.c_str(),
              _signal == Signal::Carbon ? "carbon" : "water", _smoothing_factor);
}

Greenfilling::~Greenfilling()
{
}

void Greenfilling::on_simulation_start(double date, const rapidjson::Value & batsim_config)
{
    EasyBackfilling::on_simulation_start(date, batsim_config);
    sample_intensity(date);
}

void Greenfilling::update_ema(double intensity)
{
    if (!_ema_initialized)
    {
        _ema = intensity;
        _ema_initialized = true;
        if (_greenfilling_debug)
            LOG_F(INFO, "EMA initialized to %g", _ema);
    }
    else
    {
        _ema = _smoothing_factor * intensity + (1.0 - _smoothing_factor) * _ema;
        if (_greenfilling_debug)
            LOG_F(INFO, "EMA updated to %g (current=%g)", _ema, intensity);
    }
}

void Greenfilling::sample_intensity(double date)
{
    const char * column = _signal == Signal::Carbon ? "carbon_intensity" : "water_intensity";
    double value = _intensity_source->get_value(date, column);

    if (!std::isnan(value))
    {
        _intensity = value;
        update_ema(value);
    }
}

bool Greenfilling::should_allow_backfilling() const
{
    if (!_ema_initialized)
        return true;

    return _intensity <= _ema;
}

void Greenfilling::make_decisions(double date,
                                  SortableJobOrder::UpdateInformation * update_info,
                                  SortableJobOrder::CompareInformation * compare_info)
{
    sample_intensity(date);

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

    // Determine whether backfilling is allowed based on intensity
    bool allow_backfilling = should_allow_backfilling();

    if (_greenfilling_debug)
    {
        LOG_F(INFO, "Greenfilling decision at date=%g: allow_backfilling=%d (signal=%s)",
              date, (int)allow_backfilling, _signal == Signal::Carbon ? "carbon" : "water");
        LOG_F(INFO, "  intensity=%g, ema=%g, initialized=%d", _intensity, _ema, (int)_ema_initialized);
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
