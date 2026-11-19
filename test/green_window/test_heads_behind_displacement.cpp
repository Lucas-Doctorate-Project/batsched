#include "scenario.hpp"

// Shared by the first two scenarios: 4 machines and intensity 100 until 20,
// then 1, so a 2-node job released at 0 is displaced to 20 and holds nodes 0-1
// idle until then. Behind it, a 3-node job cannot run before 30: nodes 2-3 are
// too few, and from 20 the displaced job uses 0-1 until 30. A 2-node job that
// runs for 50 fits now on nodes 2-3 and would still be running at 30.
static const std::vector<double> CLEAN_FROM_20 = { 100, 100, 1 };

static void backfill_mode_lets_a_later_job_overtake_the_blocked_head()
{
    GreenWindowScenario scenario(4, CLEAN_FROM_20, 20, 0, "backfill");
    scenario.add_job("a_displaced", 2, 10);
    scenario.add_job("b_wide", 3, 10);
    scenario.add_job("c_long", 2, 50);
    scenario.dispatch(0, { "a_displaced", "b_wide", "c_long" });
    scenario.expect_started({ "c_long" });
}

static void defaults_to_backfill_mode()
{
    GreenWindowScenario scenario(4, CLEAN_FROM_20, 20, WithoutNewOptions());
    scenario.add_job("a_displaced", 2, 10);
    scenario.add_job("b_wide", 3, 10);
    scenario.add_job("c_long", 2, 50);
    scenario.dispatch(0, { "a_displaced", "b_wide", "c_long" });
    scenario.expect_started({ "c_long" });
}

static void easy_mode_reserves_the_blocked_head_after_the_displaced_job()
{
    GreenWindowScenario scenario(4, CLEAN_FROM_20, 20, 0, "easy");
    scenario.add_job("a_displaced", 2, 10);
    scenario.add_job("b_wide", 3, 10);
    scenario.add_job("c_long", 2, 50);
    scenario.dispatch(0, { "a_displaced", "b_wide", "c_long" });
    scenario.expect_started({});
    scenario.expect_callback(20);
    require_green_window(GreenWindowSchedulingTestAccess::contention_start(*scenario.scheduler) == 30,
        "Expected b_wide reserved at 30, after the displaced job, not on the held nodes before 20");

    scenario.dispatch(20, {}, {}, true);
    scenario.expect_started({ "a_displaced" });
    scenario.dispatch(30, {}, { "a_displaced" });
    scenario.expect_started({ "b_wide" });
}

// Under a flat trace, a_short takes nodes 0-1 until 1 and b_running nodes 2-3
// until 5. Then the trace changes, and a 2-node job released at 1 is displaced
// to 20. Every node is free at 20, so its window, and its hold, is on nodes 0-1.
// The 2-node head behind it can use nodes 2-3 from 5, before the displaced start.
static void easy_mode_reserves_outside_the_held_nodes_before_the_displaced_start()
{
    GreenWindowScenario scenario(4, { 100 }, 20, 0, "easy");
    scenario.add_job("a_short", 2, 1);
    scenario.add_job("b_running", 2, 5);
    scenario.dispatch(0, { "a_short", "b_running" });
    scenario.expect_started({ "a_short", "b_running" });

    scenario.trace->values = CLEAN_FROM_20;
    scenario.add_job("c_displaced", 2, 10, 1);
    scenario.add_job("d_head", 2, 10, 1);
    scenario.dispatch(1, { "c_displaced", "d_head" }, { "a_short" });
    scenario.expect_started({});
    scenario.expect_callback(20);
    require_green_window(GreenWindowSchedulingTestAccess::contention_start(*scenario.scheduler) == 5,
        "Expected d_head reserved at 5 on the nodes outside the hold");

    scenario.dispatch(5, {}, { "b_running" });
    scenario.expect_started({ "d_head" });
    require_green_window(scenario.allocated_nodes("d_head") == "2-3", "Expected d_head on nodes 2-3");
}

// Two 2-node jobs released at 0 on 4 machines with CLEAN_FROM_20. The first is
// displaced to 20 on nodes 0-1. The second fits now on nodes 2-3, and at 20 on
// the same nodes, next to the first.
static void easy_mode_starts_the_job_behind_at_once()
{
    GreenWindowScenario scenario(4, CLEAN_FROM_20, 20, 0, "easy");
    scenario.add_job("a_displaced", 2, 10);
    scenario.add_job("b_behind", 2, 10);
    scenario.dispatch(0, { "a_displaced", "b_behind" });
    scenario.expect_started({ "b_behind" });
}

static void score_mode_displaces_the_job_behind_too()
{
    GreenWindowScenario scenario(4, CLEAN_FROM_20, 20, 0, "score");
    scenario.add_job("a_displaced", 2, 10);
    scenario.add_job("b_behind", 2, 10);
    scenario.dispatch(0, { "a_displaced", "b_behind" });
    scenario.expect_started({});
    scenario.expect_callback(20);

    scenario.dispatch(20, {}, {}, true);
    scenario.expect_started({ "a_displaced", "b_behind" });
    require_green_window(scenario.allocated_nodes("b_behind") == "2-3", "Expected b_behind on nodes 2-3");
}

// Two displacements with different starts, on 6 machines and horizon 30.
//   0: under a flat trace, a_filler takes nodes 0-3 until 1 and b_running nodes 4-5 until 40.
//   1: the trace is cleanest on [30,40), so c_late is displaced to 30. Its window,
//      and its hold, is on nodes 0-1.
//   2: the trace is now cleanest on [20,30), so d_early (walltime 5) is displaced
//      to 20, on nodes 2-3, since 0-1 are held.
//   3: e_blocked (2 nodes) fits nowhere now. Before 20 only nodes 4-5 are outside
//      the holds, busy until 40. From 20, nodes 2-3 are no longer held, and
//      d_early frees them at 25, while c_late still holds 0-1 until 30.
// So the earliest start that avoids the held nodes is 25 on nodes 2-3. Waiting
// for the last displaced start would give 30, and ignoring the hold of c_late
// once d_early starts would give 20 on its held nodes 0-1.
static void score_mode_reserves_between_two_displaced_starts()
{
    GreenWindowScenario scenario(6, { 100 }, 30, 0, "score");
    scenario.add_job("a_filler", 4, 1);
    scenario.add_job("b_running", 2, 40);
    scenario.dispatch(0, { "a_filler", "b_running" });
    scenario.expect_started({ "a_filler", "b_running" });

    scenario.trace->values = { 100, 100, 100, 1, 100 };
    scenario.add_job("c_late", 2, 10, 1);
    scenario.dispatch(1, { "c_late" }, { "a_filler" });
    scenario.expect_started({});
    scenario.expect_callback(30);

    scenario.trace->values = { 100, 100, 1, 100, 100 };
    scenario.add_job("d_early", 2, 5, 2);
    scenario.dispatch(2, { "d_early" });
    scenario.expect_started({});
    scenario.expect_callback(20);

    scenario.add_job("e_blocked", 2, 5, 3);
    scenario.dispatch(3, { "e_blocked" });
    scenario.expect_started({});
    require_green_window(GreenWindowSchedulingTestAccess::contention_start(*scenario.scheduler) == 25,
        "Expected e_blocked reserved at 25, between the two displaced starts");

    scenario.dispatch(20, {}, {}, true);
    scenario.expect_started({ "d_early" });
    scenario.dispatch(25, {}, { "d_early" });
    scenario.expect_started({ "e_blocked" });
    require_green_window(scenario.allocated_nodes("e_blocked") == "2-3", "Expected e_blocked on nodes 2-3");
    scenario.dispatch(30, {}, { "e_blocked" }, true);
    scenario.expect_started({ "c_late" });
}

void test_heads_behind_displacement_regressions()
{
    backfill_mode_lets_a_later_job_overtake_the_blocked_head();
    defaults_to_backfill_mode();
    easy_mode_reserves_the_blocked_head_after_the_displaced_job();
    easy_mode_reserves_outside_the_held_nodes_before_the_displaced_start();
    easy_mode_starts_the_job_behind_at_once();
    score_mode_displaces_the_job_behind_too();
    score_mode_reserves_between_two_displaced_starts();
}
