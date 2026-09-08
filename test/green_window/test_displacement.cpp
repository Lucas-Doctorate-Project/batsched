#include "scenario.hpp"

static void backfills_only_outside_the_displaced_nodes()
{
    GreenWindowScenario scenario(4, { 100, 100, 1 });
    scenario.add_job("displaced", 2, 10);
    scenario.add_job("outside", 2, 50);
    scenario.add_job("would_fill_idle_gap", 1, 1);
    scenario.dispatch(0, { "displaced", "outside", "would_fill_idle_gap" });
    scenario.expect_started({ "outside" });
    scenario.expect_callback(20);
    require_green_window(scenario.allocated_nodes("outside") == "2-3", "Expected backfill on nodes 2-3");
    scenario.dispatch(20, {}, {}, true);
    scenario.expect_started({ "displaced" });
    require_green_window(scenario.allocated_nodes("displaced") == "0-1", "Expected reserved nodes 0-1");
}

static void serves_arrivals_and_old_jobs_during_displacement()
{
    GreenWindowScenario scenario(4, { 100, 100, 1 });
    scenario.add_job("displaced", 2, 10);
    scenario.add_job("first_fill", 1, 50);
    scenario.add_job("waiting_fill", 2, 50);
    scenario.dispatch(0, { "displaced", "first_fill", "waiting_fill" });
    scenario.expect_started({ "first_fill" });
    scenario.add_job("arrival", 1, 50, 5);
    scenario.dispatch(5, { "arrival" });
    scenario.expect_started({ "arrival" });
    scenario.expect_callback(20, 0);
    scenario.dispatch(10, {}, { "first_fill", "arrival" });
    scenario.expect_started({ "waiting_fill" });
    scenario.expect_callback(20, 0);
    scenario.dispatch(20, {}, {}, true);
    scenario.expect_started({ "displaced" });
}

static void callbacks_release_reservations_once()
{
    GreenWindowScenario scenario(4, { 100, 100, 1 });
    const Job *displaced = scenario.add_job("displaced", 2, 10);
    scenario.dispatch(0, { "displaced" });
    scenario.dispatch(10, {}, {}, true);
    scenario.expect_started({});
    scenario.expect_callback(20, 0);
    scenario.dispatch(20, {}, {}, true);
    scenario.expect_started({ "displaced" });
    require_green_window(!GreenWindowSchedulingTestAccess::origin_retained(*scenario.scheduler, displaced),
        "Expected displacement origin erased on execution");
    scenario.dispatch(20, {}, {}, true);
    scenario.expect_started({});
    scenario.add_job("later", 2, 50, 21);
    scenario.dispatch(21, { "later" });
    scenario.expect_started({ "later" });
}

static void preserves_deadline_across_redeferrals()
{
    GreenWindowScenario scenario(2, { 100, 10, 100, 1 });
    scenario.add_job("head", 2, 10);
    scenario.dispatch(0, { "head" });
    scenario.expect_callback(10);
    GreenWindowSchedulingTestAccess::reconsider_displacement(*scenario.scheduler);
    scenario.trace->values = { 100, 100, 10, 1 };
    scenario.dispatch(5);
    scenario.expect_started({});
    scenario.expect_callback(20);
    scenario.dispatch(10, {}, {}, true);
    scenario.expect_started({});
    scenario.expect_callback(20, 0);
    scenario.dispatch(20, {}, {}, true);
    scenario.expect_started({ "head" });
}

void test_displacement_regressions()
{
    backfills_only_outside_the_displaced_nodes();
    serves_arrivals_and_old_jobs_during_displacement();
    callbacks_release_reservations_once();
    preserves_deadline_across_redeferrals();
}
