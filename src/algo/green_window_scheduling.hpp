#pragma once

#include "easy_bf.hpp"
#include "green_window_intensity.hpp"

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

class GreenWindowScheduling : public EasyBackfilling
{
public:
    /** Preserve EASY backfilling while shifting runnable heads to greener windows. Example: construct with FCFSOrder. */
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

    // A job waiting for its scored window. Its machines stay idle until start.
    struct Displacement
    {
        const Job *job = nullptr;
        Rational start = 0;
        IntervalSet machines;
    };

    // What the jobs queued behind a pending displacement do.
    enum class HeadsBehindDisplacement
    {
        Backfill, // Only the greedy backfill, with no reservation (the original behavior).
        Easy, // Plain EASY without the score: start in queue order, reserve the first blocked one.
        Score, // As in Easy, but each job that fits is scored like the queue head and may be displaced too.
    };

    static std::string get_required_string_option(rapidjson::Document *variant_options, const char *option_name);
    static std::string get_intensity_trace_option(rapidjson::Document *variant_options);
    static std::string get_signal_property_option(rapidjson::Document *variant_options);
    static double get_required_positive_double_option(rapidjson::Document *variant_options, const char *option_name);
    static double get_optional_positive_double_option(
        rapidjson::Document *variant_options, const char *option_name, double default_value);
    static bool get_optional_bool_option(
        rapidjson::Document *variant_options, const char *option_name, bool default_value);
    static double get_min_relative_gain_option(rapidjson::Document *variant_options);
    static HeadsBehindDisplacement get_heads_behind_displacement_option(rapidjson::Document *variant_options);

    Rational configured_window_step(rapidjson::Document *variant_options) const;
    void log_window_choice(const Job *job, Rational date, Rational t0, const WindowCandidate &now,
        const WindowCandidate &lowest, const WindowCandidate &chosen) const;
    void release_completed_allocations();
    void enqueue_released_jobs(double date, SortableJobOrder::UpdateInformation *update_info);
    void clear_priority_reservation();
    void schedule_runnable_heads(Rational date);
    void schedule_heads_behind_displacement(Rational date);
    void reserve_earliest_start_around_holds(const Job *job, Rational date);
    bool try_reserve_before(const Job *job, Rational after, Rational before);
    std::vector<Rational> hold_phase_boundaries(Rational date) const;
    void backfill_waiting_jobs(Rational date);
    bool try_start_immediately(const Job *job, Rational date);
    bool fits_immediately(const Job *job, Rational date) const;
    void update_schedule_present(Rational date);
    void execute_displaced_jobs_due(Rational date);
    void schedule_priority_job(Rational date);
    void schedule_at_scored_window(const Job *job, Rational date);

    bool has_pending_displacement() const;
    bool is_displaced(const Job *job) const;
    IntervalSet machines_held_at(Rational date) const;
    Schedule::JobAlloc insert_at_scored_window(const Job *job, const WindowCandidate &candidate);
    void start_job(const Job *job, const IntervalSet &machines, Rational date);
    void hold_displacement(const Job *job, const Schedule::JobAlloc &alloc);

    WindowCandidate find_best_window(const Job *job, Rational date);
    WindowCandidate find_lowest_score_window(
        const Job *job, Rational t0, Rational date, const WindowCandidate &now) const;
    WindowCandidate apply_minimum_gain(const WindowCandidate &now, const WindowCandidate &lowest) const;
    Rational first_grid_point_after(Rational date) const;
    void consider_window(const Job *job, Rational t0, Rational begin, WindowCandidate &best) const;
    bool find_exact_allocation(const Job *job, Rational begin, Rational end, IntervalSet &machines) const;
    IntervalSet available_machines_during_period(Rational begin, Rational end) const;

    double compute_window_score(const Job *job, Rational t0, Rational begin, Rational end) const;
    double intensity_sum(Rational begin, Rational end) const;

    void request_displaced_start_calls(Rational date);
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

    // Fraction in [0, 1) by which a window must cut the score of starting now to displace the job.
    double _min_relative_gain = 0.0;

    HeadsBehindDisplacement _heads_behind_displacement = HeadsBehindDisplacement::Backfill;

    // b46671e restarted dm on every search. Retain first runnable time until dispatch.
    std::map<const Job *, Rational> _displacement_origins;
    const Job *_priority_reservation = nullptr;
    // In the backfill and easy modes only the queue head is displaced, and only when none is
    // pending, so this holds at most one. The score mode can also displace the jobs behind it.
    std::vector<Displacement> _displacements;

    bool _green_window_scheduling_debug = false;
    std::set<double> _requested_call_dates;
};
