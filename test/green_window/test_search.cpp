#include "scenario.hpp"

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
    retains_the_first_runnable_origin();
    starts_displacement_clock_after_queue_contention();
    keeps_off_grid_starts_and_execution_past_deadline();
    starts_now_when_no_grid_point_is_reachable();
    holds_only_one_displacement_at_a_time();
}
