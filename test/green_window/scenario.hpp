#pragma once

#include "algo/green_window_scheduling.hpp"

#include <algorithm>
#include <loguru.hpp>
#include <stdexcept>
#include <vector>

inline void require_green_window(bool condition, const std::string &expectation)
{
    if (!condition)
        throw std::runtime_error(expectation);
}

/** Collect log lines into a std::vector<std::string>. Example: loguru::add_callback("id", capture_log, &lines, ...). */
inline void capture_log(void *records, const loguru::Message &message)
{
    static_cast<std::vector<std::string> *>(records)->push_back(message.message);
}

class FakeGreenWindowIntensity : public GreenWindowIntensity
{
public:
    explicit FakeGreenWindowIntensity(std::vector<double> blocks)
        : values(std::move(blocks))
    {
    }

    double integral(const std::string &property, double begin, double end) const override
    {
        require_green_window(property == "carbon_intensity" || property == "water_intensity",
            "Unexpected signal '" + property + "', expected carbon_intensity or water_intensity");
        double total = 0;
        while (begin < end)
        {
            std::size_t index = std::min(static_cast<std::size_t>(begin / 10), values.size() - 1);
            double boundary = index + 1 == values.size() ? end : std::min(end, (index + 1) * 10.0);
            total += (boundary - begin) * values[index];
            begin = boundary;
        }
        return total;
    }

    double sampling_period(const std::string &property) const override
    {
        (void)property;
        return 10;
    }

    std::vector<double> values;
};

struct GreenWindowSchedulingTestAccess
{
    static Rational selected_start(GreenWindowScheduling &scheduler, const Job *job, Rational date)
    {
        return scheduler.find_best_window(job, date).begin;
    }

    static void enable_debug_log(GreenWindowScheduling &scheduler)
    {
        scheduler._green_window_scheduling_debug = true;
    }

    static void reconsider_displacement(GreenWindowScheduling &scheduler)
    {
        require_green_window(scheduler._displacements.size() == 1, "Expected exactly one displacement to reconsider");
        scheduler._schedule.remove_job(scheduler._displacements.front().job);
        scheduler._displacements.clear();
    }

    static bool origin_retained(const GreenWindowScheduling &scheduler, const Job *job)
    {
        return scheduler._displacement_origins.count(job) == 1;
    }

    static Rational contention_start(const GreenWindowScheduling &scheduler)
    {
        const Job *job = scheduler._priority_reservation;
        return scheduler._schedule.find_first_occurence_of_job(job, scheduler._schedule.begin())->begin;
    }
};

/** Leave out min_relative_gain and heads_behind_displacement, as option files older than them do. */
struct WithoutNewOptions
{
};

class GreenWindowScenario
{
public:
    explicit GreenWindowScenario(int machines = 4, std::vector<double> blocks = { 100 }, double horizon = 20,
        double min_relative_gain = 0, const std::string &heads_behind_displacement = "backfill")
        : queue(&order)
        , trace(new FakeGreenWindowIntensity(std::move(blocks)))
    {
        parse_options(horizon);
        rapidjson::Document::AllocatorType &allocator = options.GetAllocator();
        options.AddMember("min_relative_gain", min_relative_gain, allocator);
        options.AddMember("heads_behind_displacement",
            rapidjson::Value(heads_behind_displacement.c_str(), allocator), allocator);
        start_scheduler(machines);
    }

    GreenWindowScenario(int machines, std::vector<double> blocks, double horizon, WithoutNewOptions)
        : queue(&order)
        , trace(new FakeGreenWindowIntensity(std::move(blocks)))
    {
        parse_options(horizon);
        start_scheduler(machines);
    }

    void parse_options(double horizon)
    {
        options.Parse(R"({"intensity_trace":"fake.csv","intensity_zone":"TESTZONE","signal":"carbon",
            "planning_horizon_seconds":20})");
        options["planning_horizon_seconds"].SetDouble(horizon);
    }

    void start_scheduler(int machines)
    {
        scheduler.reset(new GreenWindowScheduling(
            &workload, &decision, &queue, &selector, 0, &options, std::unique_ptr<GreenWindowIntensity>(trace)));
        scheduler->set_nb_machines(machines);
        scheduler->on_simulation_start(0, options);
    }

    const Job *add_job(const std::string &identifier, int machines, int walltime, double submission = 0)
    {
        const std::string description = "{\"id\":\"" + identifier + "\",\"res\":" + std::to_string(machines)
            + ",\"walltime\":" + std::to_string(walltime) + "}";
        workload.add_job_from_json_description_string(description, identifier, submission);
        return workload[identifier];
    }

    void dispatch(double date, const std::vector<std::string> &released = {},
        const std::vector<std::string> &ended = {}, bool callback = false)
    {
        decision.clear();
        scheduler->on_job_release(date, released);
        scheduler->on_job_end(date, ended);
        if (callback)
            scheduler->on_requested_call(date);
        SortableJobOrder::UpdateInformation update(date);
        scheduler->make_decisions(date, &update, nullptr);
        response.Parse(decision.content(date).c_str());
        require_green_window(!response.HasParseError(), "Expected valid scheduler protocol JSON");
        scheduler->clear_recent_data_structures();
    }

    void expect_started(const std::vector<std::string> &expected) const
    {
        std::vector<std::string> actual;
        for (const rapidjson::Value &event : response["events"].GetArray())
        {
            if (std::string(event["type"].GetString()) == "EXECUTE_JOB")
                actual.push_back(event["data"]["job_id"].GetString());
        }
        std::string expected_ids;
        for (const std::string &identifier : expected)
            expected_ids += identifier + " ";
        std::string actual_ids;
        for (const std::string &identifier : actual)
            actual_ids += identifier + " ";
        require_green_window(actual == expected, "Dispatched [" + actual_ids + "], expected [" + expected_ids + "]");
    }

    void expect_callback(double date, int count = 1) const
    {
        int matches = 0;
        for (const rapidjson::Value &event : response["events"].GetArray())
        {
            if (std::string(event["type"].GetString()) == "CALL_ME_LATER")
                matches += event["data"]["timestamp"].GetDouble() == date;
        }
        require_green_window(matches == count,
            "Unexpected callback count " + std::to_string(matches) + ", expected " + std::to_string(count) + " at "
                + std::to_string(date));
    }

    std::string allocated_nodes(const std::string &identifier) const
    {
        for (const rapidjson::Value &event : response["events"].GetArray())
        {
            if (std::string(event["type"].GetString()) == "EXECUTE_JOB"
                && event["data"]["job_id"].GetString() == identifier)
                return event["data"]["alloc"].GetString();
        }
        throw std::runtime_error("No allocation for '" + identifier + "', expected EXECUTE_JOB event");
    }

    Workload workload;
    SchedulingDecision decision;
    FCFSOrder order;
    Queue queue;
    BasicResourceSelector selector;
    rapidjson::Document options;
    FakeGreenWindowIntensity *trace;
    std::unique_ptr<GreenWindowScheduling> scheduler;
    rapidjson::Document response;
};

void test_displacement_regressions();
void test_window_search_regressions();
void test_minimum_gain_regressions();
void test_heads_behind_displacement_regressions();
