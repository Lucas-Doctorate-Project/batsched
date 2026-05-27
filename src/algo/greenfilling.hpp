#pragma once

#include <memory>

#include "csv_parser.hpp"
#include "easy_bf.hpp"

class Greenfilling : public EasyBackfilling
{
public:
    Greenfilling(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
                 double rjms_delay, rapidjson::Document * variant_options);
    virtual ~Greenfilling();

    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_config);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

private:
    void update_ema(double intensity, double & ema, bool & initialized, const char * label);
    void sample_intensities(double date);
    bool should_allow_backfilling() const;

    std::unique_ptr<CSV_Parser> _intensity_source;

    double _carbon_ema = 0.0;
    double _water_ema = 0.0;
    bool _carbon_ema_initialized = false;
    bool _water_ema_initialized = false;

    double _smoothing_factor = 0.3;
    double _ema_threshold = 1.0;
    bool _greenfilling_debug = false;
};
