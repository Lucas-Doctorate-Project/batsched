#pragma once

#include "../isalgorithm.hpp"
#include "../schedule.hpp"

/**
 * @brief Proof of Concept scheduler to demonstrate intensity data access
 *
 * This scheduler implements simple FCFS scheduling while querying and logging
 * carbon and water intensity data from Batsim. The purpose is to verify that
 * the data flow between Batsim and Batsched works correctly.
 */
class IntensityProofOfConcept : public ISchedulingAlgorithm
{
public:
    /**
     * @brief Construct a new Intensity Proof Of Concept scheduler
     */
    IntensityProofOfConcept(Workload * workload,
                           SchedulingDecision * decision,
                           Queue * queue,
                           ResourceSelector * selector,
                           double rjms_delay,
                           rapidjson::Document * variant_options);

    /**
     * @brief Destroy the Intensity Proof Of Concept scheduler
     */
    virtual ~IntensityProofOfConcept();

    /**
     * @brief Called when simulation starts - initializes schedule and queries initial intensity
     */
    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_config);

    /**
     * @brief Called when simulation ends
     */
    virtual void on_simulation_end(double date);

    /**
     * @brief Main scheduling logic - FCFS with intensity data logging
     */
    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

private:
    Schedule _schedule;  ///< Schedule data structure for managing job allocations
};
