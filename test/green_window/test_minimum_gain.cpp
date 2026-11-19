#include "scenario.hpp"

// Shared by the scenarios below: 2 machines, a 2-node job with walltime 10,
// intensity 100 on [0,10) and 90 afterwards, horizon 10. The candidates are:
//   now (begin 0):    compute 10 s at 100                  -> 320 * 1000        = 320000
//   later (begin 10): idle 10 s at 100, compute 10 s at 90 -> 10 * 1000 + 320 * 900 = 298000
// (both per node and in W·s·intensity), so waiting cuts the score by 6.875%.
static const std::vector<double> SLIGHTLY_CLEANER_LATER = { 100, 90 };

static void displaces_when_the_gain_reaches_the_minimum()
{
    GreenWindowScenario scenario(2, SLIGHTLY_CLEANER_LATER, 10, 0.05);
    scenario.add_job("head", 2, 10);
    scenario.dispatch(0, { "head" });
    scenario.expect_started({});
    scenario.expect_callback(10);
}

static void defaults_to_no_minimum_gain()
{
    GreenWindowScenario scenario(2, SLIGHTLY_CLEANER_LATER, 10, WithoutNewOptions());
    scenario.add_job("head", 2, 10);
    scenario.dispatch(0, { "head" });
    scenario.expect_started({});
    scenario.expect_callback(10);
}

static void starts_now_when_the_gain_falls_short_of_the_minimum()
{
    GreenWindowScenario scenario(2, SLIGHTLY_CLEANER_LATER, 10, 0.10);
    GreenWindowSchedulingTestAccess::enable_debug_log(*scenario.scheduler);
    scenario.add_job("head", 2, 10);

    std::vector<std::string> records;
    loguru::add_callback("minimum-gain-test", capture_log, &records, loguru::Verbosity_INFO);
    scenario.dispatch(0, { "head" });
    loguru::remove_callback("minimum-gain-test");

    scenario.expect_started({ "head" });
    require_green_window(records.size() == 1, "Expected one record, got " + std::to_string(records.size()));
    rapidjson::Document record;
    record.Parse(records[0].c_str());
    require_green_window(record["begin"].GetDouble() == 0 && record["rejected_begin"].GetDouble() == 10,
        "Expected the window at 10 rejected in favor of 0, got " + records[0]);
}

void test_minimum_gain_regressions()
{
    displaces_when_the_gain_reaches_the_minimum();
    defaults_to_no_minimum_gain();
    starts_now_when_the_gain_falls_short_of_the_minimum();
}
