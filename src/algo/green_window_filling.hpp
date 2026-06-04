#pragma once

#include "easy_bf.hpp"
#include "./csv_parser.hpp"

#include <set>
#include <string>

class GreenWindowFilling : public EasyBackfilling
{
public:
    GreenWindowFilling(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
                       double rjms_delay, rapidjson::Document * variant_options);
    virtual ~GreenWindowFilling();

    virtual void on_requested_call(double date);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

private:
    struct WindowCandidate
    {
        bool found = false;
        Rational begin = 0;
        Rational end = 0;
        double score = 0.0;
        IntervalSet machines;
    };

    static std::string get_required_string_option(rapidjson::Document * variant_options,
                                                  const char * option_name);
    static std::string get_intensity_trace_option(rapidjson::Document * variant_options);
    static double get_required_positive_double_option(rapidjson::Document * variant_options,
                                                      const char * option_name);
    static double get_optional_nonnegative_double_option(rapidjson::Document * variant_options,
                                                        const char * option_name,
                                                        double default_value);
    static bool get_optional_bool_option(rapidjson::Document * variant_options,
                                         const char * option_name,
                                         bool default_value);

    void update_schedule_present(Rational date);
    void execute_ready_jobs(double date);
    void reserve_unscheduled_jobs(Rational date);

    Schedule::JobAlloc reserve_job(const Job * job, Rational date);
    WindowCandidate find_best_window(const Job * job, Rational date) const;
    bool find_exact_allocation(const Job * job, Rational begin, Rational end, IntervalSet & machines) const;
    IntervalSet available_machines_during_period(Rational begin, Rational end) const;

    double compute_window_score(const Job * job, Rational begin, Rational end) const;
    double normalize_intensity_sum(double intensity_sum, double min_intensity,
                                   double max_intensity, double duration) const;

    void request_earliest_future_reservation_call(Rational date);
    void forget_requested_call_date(double date);
    bool is_call_date_already_requested(double date) const;
    static bool same_date(double left, double right);

private:
    std::string _intensity_trace;
    std::string _intensity_zone;
    CSV_Parser _csv_parser;

    Rational _planning_horizon = 0;
    Rational _window_step = 0;
    double _carbon_weight = 1.0;
    double _water_weight = 1.0;

    bool _green_window_filling_debug = false;
    std::set<double> _requested_call_dates;
};
