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
    void update_ema(double intensity);
    void sample_intensity(double date);
    bool should_allow_backfilling() const;

    enum class Signal { Carbon, Water };

    std::unique_ptr<CSV_Parser> _intensity_source;
    Signal _signal = Signal::Carbon;

    double _intensity = 0.0;
    double _ema = 0.0;
    bool _ema_initialized = false;

    double _smoothing_factor = 0.3;
    bool _greenfilling_debug = false;
};
