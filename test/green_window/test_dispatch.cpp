#include "scenario.hpp"

#include <iostream>

static void dispatches_every_immediately_runnable_head()
{
    GreenWindowScenario scenario(1600);
    std::vector<std::string> identifiers;
    for (int index = 0; index < 16; ++index)
    {
        identifiers.push_back("job" + std::to_string(index));
        scenario.add_job(identifiers.back(), 100, 100);
    }
    std::sort(identifiers.begin(), identifiers.end());
    scenario.dispatch(0, identifiers);
    scenario.expect_started(identifiers);
    require_green_window(scenario.queue.is_empty(), "Expected all 1600 nodes occupied after a single event");
}

static void backfills_without_delaying_the_blocked_head()
{
    GreenWindowScenario scenario;
    scenario.add_job("running", 3, 20);
    scenario.dispatch(0, { "running" });
    scenario.add_job("a_head", 4, 20);
    scenario.add_job("b_too_long", 1, 21);
    scenario.add_job("c_fits", 1, 19);
    scenario.dispatch(1, { "a_head", "b_too_long", "c_fits" });
    scenario.expect_started({ "c_fits" });
    require_green_window(GreenWindowSchedulingTestAccess::contention_start(*scenario.scheduler) == 20,
        "Expected EASY head reservation to remain at 20 after backfilling");
    scenario.dispatch(20, {}, { "running", "c_fits" });
    scenario.expect_started({ "a_head" });
}

static void retries_old_backfill_jobs_after_early_completion()
{
    GreenWindowScenario scenario;
    scenario.add_job("running", 3, 20);
    scenario.add_job("short", 1, 10);
    scenario.dispatch(0, { "running", "short" });
    scenario.add_job("head", 4, 20);
    scenario.add_job("waiting", 1, 5);
    scenario.dispatch(1, { "head", "waiting" });
    scenario.expect_started({});
    scenario.dispatch(5, {}, { "short" });
    scenario.expect_started({ "waiting" });
    scenario.dispatch(10, {}, { "running", "waiting" });
    scenario.expect_started({ "head" });
}

static void rejects_invalid_jobs_without_blocking_dispatch()
{
    GreenWindowScenario scenario;
    scenario.add_job("oversized", 5, 10);
    scenario.add_job("missing_walltime", 1, -1);
    scenario.workload["missing_walltime"]->has_walltime = false;
    scenario.add_job("valid", 4, 10);
    scenario.dispatch(0, { "oversized", "missing_walltime", "valid" });
    scenario.expect_started({ "valid" });
    require_green_window(scenario.response["events"].Size() == 3, "Expected two rejections and one execution");
    require_green_window(scenario.queue.is_empty(), "Expected invalid jobs to leave the queue");
}

int main()
{
    try
    {
        dispatches_every_immediately_runnable_head();
        backfills_without_delaying_the_blocked_head();
        retries_old_backfill_jobs_after_early_completion();
        rejects_invalid_jobs_without_blocking_dispatch();
        test_displacement_regressions();
        test_window_search_regressions();
        std::cout << "Green-window regressions passed\n";
        return 0;
    }
    catch (const std::exception &failure)
    {
        std::cerr << failure.what() << '\n';
        return 1;
    }
}
