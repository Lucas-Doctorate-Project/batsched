#pragma once

#include "csv_parser.hpp"
#include "easy_bf.hpp"

class Cats : public EasyBackfilling
{
public:
    Cats(Workload *workload, SchedulingDecision *decision, Queue *queue, ResourceSelector *selector,
         double rjms_delay, rapidjson::Document *options);

    void make_decisions(double date, SortableJobOrder::UpdateInformation *update_info,
                        SortableJobOrder::CompareInformation *compare_info) override;

private:
    void enqueue_job(const Job *job, SortableJobOrder::UpdateInformation *update_info) override;
    double eligible_at(const Job &job) const;

    CSV_Parser _history;
    bool _previous_day;
    double _step;
    double _max_delay;
    std::map<double, std::vector<std::string>> _pending;
};
