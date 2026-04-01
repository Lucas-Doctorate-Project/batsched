#pragma once

#include "easy_bf.hpp"
#include "greenfilling_math.hpp"

class Greenfilling : public EasyBackfilling
{
public:
    Greenfilling(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
                 double rjms_delay, rapidjson::Document * variant_options);
    virtual ~Greenfilling();

    virtual void on_answer_carbon_intensity(double date, double carbon_intensity);
    virtual void on_answer_water_intensity(double date, double water_intensity);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

private:
    void query_intensities_if_needed(double date);
    int compute_backfill_machines() const;

    double _tau        = 0.1;
    double _carbon_min = 0.0;
    double _carbon_max = 1.0;
    double _water_min  = 0.0;
    double _water_max  = 1.0;

    bool _greenfilling_debug = false;
};
