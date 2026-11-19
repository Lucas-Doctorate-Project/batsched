#include "scenario.hpp"

#include <cmath>

static void logs_decision_origin_and_immediate_score()
{
    GreenWindowScenario scenario(2, { 100, 100, 1 }, 20);
    GreenWindowSchedulingTestAccess::enable_debug_log(*scenario.scheduler);
    const Job *job = scenario.add_job("head", 2, 10);
    std::vector<std::string> records;
    loguru::add_callback("green-window-test", capture_log, &records, loguru::Verbosity_INFO);
    GreenWindowSchedulingTestAccess::selected_start(*scenario.scheduler, job, 0);
    GreenWindowSchedulingTestAccess::selected_start(*scenario.scheduler, job, 5);
    loguru::remove_callback("green-window-test");
    require_green_window(records.size() == 2, "Expected one record per search, got " + std::to_string(records.size()));

    rapidjson::Document first;
    first.Parse(records[0].c_str());
    require_green_window(first["date"].GetDouble() == 0 && first["t0"].GetDouble() == 0 && first["begin"].GetDouble() == 20,
        "Expected a decision at 0 displaced to 20, got " + records[0]);
    // Now: 2 nodes compute 10 s at 100. At 20: they idle 20 s at 100, then compute 10 s at 1.
    require_green_window(std::abs(first["now_impact"].GetDouble() - 2 * 320 * 10 * 100 / 3.6e6) < 1e-12,
        "Unexpected now_impact in " + records[0]);
    require_green_window(std::abs(first["impact"].GetDouble() - 2 * (10 * 20 * 100 + 320 * 10 * 1) / 3.6e6) < 1e-12,
        "Unexpected impact in " + records[0]);

    rapidjson::Document second;
    second.Parse(records[1].c_str());
    require_green_window(second["date"].GetDouble() == 5 && second["t0"].GetDouble() == 0,
        "Expected the retained origin 0 at date 5, got " + records[1]);
}

static void retains_the_first_runnable_origin()
{
    GreenWindowScenario scenario(2, { 100, 100, 10, 1 }, 20);
    const Job *job = scenario.add_job("head", 2, 10);
    for (double date : { 0, 5, 10, 15, 20 })
    {
        Rational selected = GreenWindowSchedulingTestAccess::selected_start(*scenario.scheduler, job, date);
        require_green_window(selected == 20, "Expected absolute deadline 20, got " + std::to_string((double)selected));
    }
    Rational expired = GreenWindowSchedulingTestAccess::selected_start(*scenario.scheduler, job, 25);
    require_green_window(expired == 25, "Expected immediate execution at 25 after deadline expiry");
}

static void starts_displacement_clock_after_queue_contention()
{
    GreenWindowScenario scenario(2, { 100, 100, 100, 10, 1 }, 10);
    const Job *running = scenario.add_job("running", 2, 30);
    scenario.trace->values = { 100 };
    scenario.dispatch(0, { "running" });
    const Job *head = scenario.add_job("head", 2, 10);
    scenario.dispatch(1, { "head" });
    require_green_window(!GreenWindowSchedulingTestAccess::origin_retained(*scenario.scheduler, head),
        "Expected no displacement clock before the head fits");
    scenario.trace->values = { 100, 100, 100, 100, 1 };
    scenario.dispatch(30, {}, { running->id });
    scenario.expect_started({});
    scenario.expect_callback(40);
    scenario.dispatch(40, {}, {}, true);
    scenario.expect_started({ "head" });
}

static void keeps_off_grid_starts_and_execution_past_deadline()
{
    GreenWindowScenario scenario(2, { 100, 100, 1, 100, 100 }, 17);
    scenario.add_job("off_grid", 2, 10, 3);
    scenario.dispatch(3, { "off_grid" });
    scenario.expect_callback(20);
    scenario.dispatch(20, {}, {}, true);
    scenario.expect_started({ "off_grid" });
    scenario.dispatch(30, {}, { "off_grid" });
    scenario.expect_started({});
}

static void starts_now_when_no_grid_point_is_reachable()
{
    GreenWindowScenario scenario(2, { 100, 1 }, 5);
    scenario.add_job("head", 2, 10);
    scenario.dispatch(0, { "head" });
    scenario.expect_started({ "head" });
}

static void holds_only_one_displacement_at_a_time()
{
    GreenWindowScenario scenario(2, { 100, 100, 1 }, 20);
    const Job *first = scenario.add_job("first", 2, 10);
    const Job *second = scenario.add_job("second", 2, 10);
    scenario.dispatch(0, { "first", "second" });
    require_green_window(GreenWindowSchedulingTestAccess::origin_retained(*scenario.scheduler, first),
        "Expected first head to own a displacement origin");
    require_green_window(!GreenWindowSchedulingTestAccess::origin_retained(*scenario.scheduler, second),
        "Expected second job to wait without displacement age");
    scenario.dispatch(20, {}, {}, true);
    scenario.expect_started({ "first" });
    scenario.dispatch(30, {}, { "first" });
    scenario.expect_started({ "second" });
}

void test_window_search_regressions()
{
    logs_decision_origin_and_immediate_score();
    retains_the_first_runnable_origin();
    starts_displacement_clock_after_queue_contention();
    keeps_off_grid_starts_and_execution_past_deadline();
    starts_now_when_no_grid_point_is_reachable();
    holds_only_one_displacement_at_a_time();
}
