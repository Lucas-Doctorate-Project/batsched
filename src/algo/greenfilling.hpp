#pragma once

#include "easy_bf.hpp"

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
    void update_carbon_ema(double carbon_intensity);
    void update_water_ema(double water_intensity);
    void query_intensities_if_needed(double date);
    bool should_allow_backfilling() const;

    double _carbon_ema = 0.0;
    double _water_ema = 0.0;
    bool _carbon_ema_initialized = false;
    bool _water_ema_initialized = false;

    double _alpha = 0.3;
    bool _query_on_new_jobs = true;
    bool _greenfilling_debug = false;
};
