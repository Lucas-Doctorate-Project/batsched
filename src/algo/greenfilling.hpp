#pragma once

#include "easy_bf.hpp"
#include "./csv_parser.hpp"

class Greenfilling : public EasyBackfilling
{
public:
    Greenfilling(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
                 double rjms_delay, rapidjson::Document * variant_options);
    virtual ~Greenfilling();

    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_config);

    virtual void on_answer_carbon_intensity(double date, double carbon_intensity);
    virtual void on_answer_water_intensity(double date, double water_intensity);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

private:
    void query_intensities_if_needed(double date);
    bool should_allow_backfilling(double date) const;

    std::string _typical_intensities_file;
    CSV_Parser _csv_parser;

    bool _greenfilling_debug = false;
};
