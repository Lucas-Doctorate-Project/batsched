#include "intensity_poc.hpp"

#include <loguru.hpp>

IntensityProofOfConcept::IntensityProofOfConcept(Workload * workload,
                                                 SchedulingDecision * decision,
                                                 Queue * queue,
                                                 ResourceSelector * selector,
                                                 double rjms_delay,
                                                 rapidjson::Document * variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{
    // Minimal setup - no special configuration needed for proof of concept
    LOG_F(INFO, "IntensityProofOfConcept scheduler initialized");
}

IntensityProofOfConcept::~IntensityProofOfConcept()
{
    // Cleanup
}

void IntensityProofOfConcept::on_simulation_start(double date,
                                                  const rapidjson::Value & batsim_config)
{
    LOG_F(INFO, "=== Intensity Proof of Concept - Simulation Starting ===");
    LOG_F(INFO, "Number of machines: %d", _nb_machines);

    // Initialize the Schedule object with the number of machines
    _schedule = Schedule(_nb_machines, date);
    (void) batsim_config;

    // Query initial carbon and water intensity to demonstrate the API works
    LOG_F(INFO, "Querying initial intensity data...");
    _decision->add_query_carbon_intensity(date);
    _decision->add_query_water_intensity(date);
}

void IntensityProofOfConcept::on_simulation_end(double date)
{
    LOG_F(INFO, "=== Intensity Proof of Concept - Simulation Ending ===");
    LOG_F(INFO, "Final carbon intensity: %g g CO2/kWh", _carbon_intensity);
    LOG_F(INFO, "Final water intensity: %g L/kWh", _water_intensity);
}

void IntensityProofOfConcept::make_decisions(double date,
                                            SortableJobOrder::UpdateInformation * update_info,
                                            SortableJobOrder::CompareInformation * compare_info)
{
    // 1. Update schedule to current time
    _schedule.update_first_slice(date);

    // 2. Remove completed jobs from schedule
    for (const std::string & job_id : _jobs_ended_recently)
    {
        Job * job = (*_workload)[job_id];
        LOG_F(INFO, "Job %s completed", job_id.c_str());
        _schedule.remove_job(job);
    }

    // 3. Query intensity factors when jobs arrive (KEY PROOF OF CONCEPT)
    if (!_jobs_released_recently.empty())
    {
        LOG_F(INFO, "New jobs arrived (%zu jobs) - querying intensity data",
              _jobs_released_recently.size());
        _decision->add_query_carbon_intensity(date);
        _decision->add_query_water_intensity(date);
    }

    // 4. Handle newly released jobs (reject if too large, queue otherwise)
    for (const std::string & job_id : _jobs_released_recently)
    {
        Job * job = (*_workload)[job_id];

        if (job->nb_requested_resources > _nb_machines)
        {
            LOG_F(WARNING, "Job %s requests %d resources but only %d machines available - REJECTING",
                  job_id.c_str(), job->nb_requested_resources, _nb_machines);
            _decision->add_reject_job(job_id, date);
        }
        else
        {
            LOG_F(INFO, "Job %s added to queue (requests %d resources)",
                  job_id.c_str(), job->nb_requested_resources);
            _queue->append_job(job, update_info);
        }
    }

    // 5. LOG INTENSITY UPDATES - THIS IS THE CORE PROOF OF CONCEPT
    if (_carbon_intensity_updated_recently)
    {
        LOG_F(INFO, "SUCCESS: Received carbon intensity = %g g CO2/kWh",
              _carbon_intensity);
    }

    if (_water_intensity_updated_recently)
    {
        LOG_F(INFO, "SUCCESS: Received water intensity = %g L/kWh",
              _water_intensity);
    }

    // 6. Schedule jobs using simple FCFS (no complex logic needed)
    // Try to schedule jobs in queue order
    const Job * job = _queue->first_job_or_nullptr();
    while (job != nullptr)
    {
        // Try to allocate this job using first-fit
        Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job, _selector);

        if (alloc.started_in_first_slice)
        {
            // We have resources available now - schedule it
            LOG_F(INFO, "Scheduling job %s on machines %s at time %g",
                  job->id.c_str(),
                  alloc.used_machines.to_string_hyphen().c_str(),
                  date);

            _decision->add_execute_job(job->id, alloc.used_machines, date);
            _queue->remove_job(job);

            // Get next job
            job = _queue->first_job_or_nullptr();
        }
        else
        {
            // Job doesn't fit right now, stop trying (FCFS - no backfilling)
            break;
        }
    }
}
