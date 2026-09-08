#pragma once

#include "easy_bf.hpp"
#include "green_window_intensity.hpp"

#include <map>
#include <memory>
#include <set>
#include <string>

class GreenWindowScheduling : public EasyBackfilling
{
public:
    /** Preserve EASY backfilling while shifting one runnable head. Example: construct with FCFSOrder. */
    GreenWindowScheduling(Workload *workload, SchedulingDecision *decision, Queue *queue, ResourceSelector *selector,
        double rjms_delay, rapidjson::Document *variant_options);
    /** Inject the trace source for deterministic scheduling. Example: pass a FakeGreenWindowIntensity. */
    GreenWindowScheduling(Workload *workload, SchedulingDecision *decision, Queue *queue, ResourceSelector *selector,
        double rjms_delay, rapidjson::Document *variant_options, std::unique_ptr<GreenWindowIntensity> intensity);
    virtual ~GreenWindowScheduling();

    /** Retire delivered callbacks. Example: on_requested_call(3600). */
    virtual void on_requested_call(double date);

    /** Dispatch runnable jobs and preserve reservations. Example: call after on_job_release. */
    virtual void make_decisions(double date, SortableJobOrder::UpdateInformation *update_info,
        SortableJobOrder::CompareInformation *compare_info);

private:
    friend struct GreenWindowSchedulingTestAccess;

    struct WindowCandidate
    {
        bool found = false;
        Rational begin = 0;
        Rational end = 0;
        double score = 0.0;
        IntervalSet machines;
    };

    static std::string get_required_string_option(rapidjson::Document *variant_options, const char *option_name);
    static std::string get_intensity_trace_option(rapidjson::Document *variant_options);
    static std::string get_signal_property_option(rapidjson::Document *variant_options);
    static double get_required_positive_double_option(rapidjson::Document *variant_options, const char *option_name);
    static double get_optional_positive_double_option(
        rapidjson::Document *variant_options, const char *option_name, double default_value);
    static bool get_optional_bool_option(
        rapidjson::Document *variant_options, const char *option_name, bool default_value);

    Rational configured_window_step(rapidjson::Document *variant_options) const;
    void log_window_choice(const Job *job, const WindowCandidate &candidate) const;
    void release_completed_allocations();
    void enqueue_released_jobs(double date, SortableJobOrder::UpdateInformation *update_info);
    void clear_priority_reservation();
    void schedule_runnable_heads(Rational date);
    void backfill_waiting_jobs(Rational date);
    void try_backfill_job(const Job *job, Rational date);
    void update_schedule_present(Rational date);
    void execute_reserved_job_if_ready(Rational date);
    void schedule_priority_job(Rational date);

    bool has_pending_displacement() const;
    Schedule::JobAlloc insert_at_scored_window(const Job *job, const WindowCandidate &candidate);
    void start_job(const Job *job, const IntervalSet &machines, Rational date);
    void hold_displaced_reservation(const Job *job, const Schedule::JobAlloc &alloc);
    void clear_reservation();

    WindowCandidate find_best_window(const Job *job, Rational date);
    Rational first_grid_point_after(Rational date) const;
    void consider_window(const Job *job, Rational t0, Rational begin, WindowCandidate &best) const;
    bool find_exact_allocation(const Job *job, Rational begin, Rational end, IntervalSet &machines) const;
    IntervalSet available_machines_during_period(Rational begin, Rational end) const;

    double compute_window_score(const Job *job, Rational t0, Rational begin, Rational end) const;
    double intensity_sum(Rational begin, Rational end) const;

    void request_reservation_call(Rational date);
    void forget_requested_call_date(double date);
    bool is_call_date_already_requested(double date) const;
    static bool same_date(double left, double right);

private:
    std::string _intensity_trace;
    std::string _intensity_zone;
    std::unique_ptr<GreenWindowIntensity> _intensity;

    std::string _signal_property;

    Rational _planning_horizon = 0;
    Rational _window_step = 0;

    double _computing_watts = 0.0;
    double _idle_watts = 0.0;

    // b46671e restarted dm on every search. Retain first runnable time until dispatch.
    std::map<const Job *, Rational> _displacement_origins;
    const Job *_priority_reservation = nullptr;
    const Job *_reserved_job = nullptr;
    Rational _reserved_start = 0;
    IntervalSet _reserved_machines;

    bool _green_window_scheduling_debug = false;
    std::set<double> _requested_call_dates;
};
