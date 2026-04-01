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
    if (variant_options->HasMember("tau"))
        _tau = (*variant_options)["tau"].GetDouble();

    if (variant_options->HasMember("carbon_min"))
        _carbon_min = (*variant_options)["carbon_min"].GetDouble();

    if (variant_options->HasMember("carbon_max"))
        _carbon_max = (*variant_options)["carbon_max"].GetDouble();

    if (variant_options->HasMember("water_min"))
        _water_min = (*variant_options)["water_min"].GetDouble();

    if (variant_options->HasMember("water_max"))
        _water_max = (*variant_options)["water_max"].GetDouble();

    if (variant_options->HasMember("greenfilling_debug"))
        _greenfilling_debug = (*variant_options)["greenfilling_debug"].GetBool();

    if (_greenfilling_debug)
        LOG_F(INFO, "Greenfilling initialized with tau=%g, carbon=[%g,%g], water=[%g,%g]",
              _tau, _carbon_min, _carbon_max, _water_min, _water_max);
}

Greenfilling::~Greenfilling()
{
}

void Greenfilling::on_answer_carbon_intensity(double date, double carbon_intensity)
{
    ISchedulingAlgorithm::on_answer_carbon_intensity(date, carbon_intensity);
}

void Greenfilling::on_answer_water_intensity(double date, double water_intensity)
{
    ISchedulingAlgorithm::on_answer_water_intensity(date, water_intensity);
}

void Greenfilling::query_intensities_if_needed(double date)
{
    if (!_jobs_released_recently.empty() || !_jobs_ended_recently.empty())
    {
        _decision->add_query_carbon_intensity(date);
        _decision->add_query_water_intensity(date);
    }
}

int Greenfilling::compute_backfill_machines() const
{
    int N_a = static_cast<int>(_schedule.begin()->available_machines.size());
    return ::compute_backfill_machines(N_a,
        _carbon_intensity, _carbon_min, _carbon_max,
        _water_intensity, _water_min, _water_max,
        _tau);
}

void Greenfilling::make_decisions(double date,
                                  SortableJobOrder::UpdateInformation * update_info,
                                  SortableJobOrder::CompareInformation * compare_info)
{
    // Query intensities on any scheduling event (job arrival or completion)
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

    // Compute how many machines are available for backfilling
    int backfill_budget = compute_backfill_machines();

    if (_greenfilling_debug)
    {
        LOG_F(INFO, "Greenfilling decision at date=%g: backfill_budget=%d", date, backfill_budget);
        LOG_F(INFO, "  Carbon: current=%g, range=[%g,%g]", _carbon_intensity, _carbon_min, _carbon_max);
        LOG_F(INFO, "  Water:  current=%g, range=[%g,%g]", _water_intensity, _water_min, _water_max);
    }

    // If no resources have been released, try to backfill newly-queued jobs
    if (_jobs_ended_recently.empty())
    {
        int nb_available_machines = static_cast<int>(_schedule.begin()->available_machines.size());

        for (unsigned int i = 0; i < recently_queued_jobs.size() && nb_available_machines > 0 && backfill_budget > 0; ++i)
        {
            const string & new_job_id = recently_queued_jobs[i];
            const Job * new_job = (*_workload)[new_job_id];

            if (_queue->contains_job(new_job) &&
                new_job != priority_job_after &&
                new_job->nb_requested_resources <= nb_available_machines &&
                new_job->nb_requested_resources <= backfill_budget)
            {
                Schedule::JobAlloc alloc = _schedule.add_job_first_fit(new_job, _selector);
                if (alloc.started_in_first_slice)
                {
                    _decision->add_execute_job(new_job_id, alloc.used_machines, date);
                    _queue->remove_job(new_job);
                    nb_available_machines -= new_job->nb_requested_resources;
                    backfill_budget -= new_job->nb_requested_resources;
                }
                else
                    _schedule.remove_job(new_job);
            }
        }
    }
    else
    {
        // Some resources have been released; traverse the whole queue.
        auto job_it = _queue->begin();
        int nb_available_machines = static_cast<int>(_schedule.begin()->available_machines.size());

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
            else if (backfill_budget > 0 && job->nb_requested_resources <= backfill_budget)
            {
                Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job, _selector);

                if (alloc.started_in_first_slice)
                {
                    _decision->add_execute_job(job->id, alloc.used_machines, date);
                    backfill_budget -= job->nb_requested_resources;
                    job_it = _queue->remove_job(job_it);
                }
                else
                {
                    _schedule.remove_job(job);
                    ++job_it;
                }
            }
            else // Budget exhausted or job too large for remaining budget
            {
                ++job_it;
            }
        }
    }
}
