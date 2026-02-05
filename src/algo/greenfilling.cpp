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
    if (variant_options->HasMember("alpha"))
        _alpha = (*variant_options)["alpha"].GetDouble();

    if (variant_options->HasMember("query_on_new_jobs"))
        _query_on_new_jobs = (*variant_options)["query_on_new_jobs"].GetBool();

    if (variant_options->HasMember("greenfilling_debug"))
        _greenfilling_debug = (*variant_options)["greenfilling_debug"].GetBool();

    if (_greenfilling_debug)
        LOG_F(INFO, "Greenfilling initialized with alpha=%g, query_on_new_jobs=%d", _alpha, (int)_query_on_new_jobs);
}

Greenfilling::~Greenfilling()
{
}

void Greenfilling::on_simulation_start(double date, const rapidjson::Value & batsim_config)
{
    EasyBackfilling::on_simulation_start(date, batsim_config);
}

void Greenfilling::on_answer_carbon_intensity(double date, double carbon_intensity)
{
    ISchedulingAlgorithm::on_answer_carbon_intensity(date, carbon_intensity);
    update_carbon_ema(carbon_intensity);
}

void Greenfilling::on_answer_water_intensity(double date, double water_intensity)
{
    ISchedulingAlgorithm::on_answer_water_intensity(date, water_intensity);
    update_water_ema(water_intensity);
}

void Greenfilling::update_carbon_ema(double carbon_intensity)
{
    if (!_carbon_ema_initialized)
    {
        _carbon_ema = carbon_intensity;
        _carbon_ema_initialized = true;
        if (_greenfilling_debug)
            LOG_F(INFO, "Carbon EMA initialized to %g", _carbon_ema);
    }
    else
    {
        _carbon_ema = _alpha * carbon_intensity + (1.0 - _alpha) * _carbon_ema;
        if (_greenfilling_debug)
            LOG_F(INFO, "Carbon EMA updated to %g (current=%g)", _carbon_ema, carbon_intensity);
    }
}

void Greenfilling::update_water_ema(double water_intensity)
{
    if (!_water_ema_initialized)
    {
        _water_ema = water_intensity;
        _water_ema_initialized = true;
        if (_greenfilling_debug)
            LOG_F(INFO, "Water EMA initialized to %g", _water_ema);
    }
    else
    {
        _water_ema = _alpha * water_intensity + (1.0 - _alpha) * _water_ema;
        if (_greenfilling_debug)
            LOG_F(INFO, "Water EMA updated to %g (current=%g)", _water_ema, water_intensity);
    }
}

void Greenfilling::query_intensities_if_needed(double date)
{
    if (_query_on_new_jobs && !_jobs_released_recently.empty())
    {
        _decision->add_query_carbon_intensity(date);
        _decision->add_query_water_intensity(date);
    }
}

bool Greenfilling::should_allow_backfilling() const
{
    if (!_carbon_ema_initialized && !_water_ema_initialized)
        return true;

    if (_carbon_ema_initialized && !_water_ema_initialized)
        return _carbon_intensity <= _carbon_ema;

    if (!_carbon_ema_initialized && _water_ema_initialized)
        return _water_intensity <= _water_ema;

    // Both initialized: allow if either metric is at or below its EMA
    return (_carbon_intensity <= _carbon_ema) || (_water_intensity <= _water_ema);
}

void Greenfilling::make_decisions(double date,
                                  SortableJobOrder::UpdateInformation * update_info,
                                  SortableJobOrder::CompareInformation * compare_info)
{
    // Query intensities when new jobs arrive
    query_intensities_if_needed(date);

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
        LOG_F(INFO, "Greenfilling decision at date=%g: allow_backfilling=%d", date, (int)allow_backfilling);
        LOG_F(INFO, "  Carbon: current=%g, ema=%g, initialized=%d", _carbon_intensity, _carbon_ema, (int)_carbon_ema_initialized);
        LOG_F(INFO, "  Water: current=%g, ema=%g, initialized=%d", _water_intensity, _water_ema, (int)_water_ema_initialized);
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
