#include "greenfilling.hpp"

#include <loguru.hpp>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "../pempek_assert.hpp"

using namespace std;

Greenfilling::Greenfilling(Workload * workload,
                           SchedulingDecision * decision,
                           Queue * queue,
                           ResourceSelector * selector,
                           double rjms_delay,
                           rapidjson::Document * variant_options) :
    EasyBackfilling(workload, decision, queue, selector, rjms_delay, variant_options),
    _typical_intensities_file(
        variant_options->HasMember("typical_intensities_file")
            ? (*variant_options)["typical_intensities_file"].GetString()
            : ""),
    _csv_parser(_typical_intensities_file)
{
    if (variant_options->HasMember("greenfilling_debug"))
        _greenfilling_debug = (*variant_options)["greenfilling_debug"].GetBool();

    if (_greenfilling_debug)
        LOG_F(INFO, "Greenfilling initialized with typical_intensitites_file=%s", _typical_intensities_file.c_str());
}

Greenfilling::~Greenfilling()
{
}

void Greenfilling::on_simulation_start(double date, const rapidjson::Value & batsim_config)
{
    EasyBackfilling::on_simulation_start(date, batsim_config);
    // Bootstrap the EMA before the first job arrives
    _decision->add_query_carbon_intensity(date);
    _decision->add_query_water_intensity(date);        
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

bool Greenfilling::should_allow_backfilling(double date) const
{
    std::pair<double,double> typical_intensities = _csv_parser.get_intensities(date);
    double carbon_threshold = typical_intensities.first;
    double water_threshold = typical_intensities.second;

    if (_greenfilling_debug)
        LOG_F(INFO, "typical_intensities(%.3lf, %.3lf); current_intensities(%.3lf, %.3lf)", carbon_threshold, water_threshold, _carbon_intensity, _water_intensity);

    // Both initialized: allow only if both metrics are at or below threshold 
    return (_carbon_intensity <= carbon_threshold) &&
           (_water_intensity <= water_threshold);
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

    // Determine whether backfilling is allowed based on intensity
    bool allow_backfilling = should_allow_backfilling(date);

    if (_greenfilling_debug)
    {
        LOG_F(INFO, "Greenfilling decision at date=%g: allow_backfilling=%d", date, (int)allow_backfilling);
        LOG_F(INFO, "  Carbon: current=%g", _carbon_intensity);
        LOG_F(INFO, "  Water: current=%g", _water_intensity);
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
