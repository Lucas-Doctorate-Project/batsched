#include "green_window_scheduling.hpp"
#include "forecast_mode.hpp"

#include <cmath>
#include <loguru.hpp>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "../pempek_assert.hpp"

namespace
{
const std::string CARBON_INTENSITY_PROPERTY = "carbon_intensity";
const std::string WATER_INTENSITY_PROPERTY = "water_intensity";

const double DEFAULT_COMPUTING_WATTS = 320.0;
const double DEFAULT_IDLE_WATTS = 10.0;
}

GreenWindowScheduling::GreenWindowScheduling(Workload *workload, SchedulingDecision *decision, Queue *queue,
    ResourceSelector *selector, double rjms_delay, rapidjson::Document *variant_options)
    : GreenWindowScheduling(workload, decision, queue, selector, rjms_delay, variant_options,
          std::unique_ptr<GreenWindowIntensity>(new CsvGreenWindowIntensity(get_intensity_trace_option(variant_options),
              get_required_string_option(variant_options, "intensity_zone"), uses_previous_day(*variant_options, false))))
{
}

GreenWindowScheduling::GreenWindowScheduling(Workload *workload, SchedulingDecision *decision, Queue *queue,
    ResourceSelector *selector, double rjms_delay, rapidjson::Document *variant_options,
    std::unique_ptr<GreenWindowIntensity> intensity)
    : EasyBackfilling(workload, decision, queue, selector, rjms_delay, variant_options)
    , _intensity_trace(get_intensity_trace_option(variant_options))
    , _intensity_zone(get_required_string_option(variant_options, "intensity_zone"))
    , _intensity(std::move(intensity))
    , _signal_property(get_signal_property_option(variant_options))
    , _planning_horizon(get_required_positive_double_option(variant_options, "planning_horizon_seconds"))
    , _computing_watts(get_optional_positive_double_option(variant_options, "computing_watts", DEFAULT_COMPUTING_WATTS))
    , _idle_watts(get_optional_positive_double_option(variant_options, "idle_watts", DEFAULT_IDLE_WATTS))
    , _min_relative_gain(get_min_relative_gain_option(variant_options))
    , _heads_behind_displacement(get_heads_behind_displacement_option(variant_options))
    , _green_window_scheduling_debug(get_optional_bool_option(variant_options, "green_window_scheduling_debug", false))
{
    PPK_ASSERT_ERROR(_intensity != nullptr, "Invalid intensity source: got null, expected GreenWindowIntensity");
    _window_step = configured_window_step(variant_options);
}

Rational GreenWindowScheduling::configured_window_step(rapidjson::Document *variant_options) const
{
    // Candidates land on absolute multiples of this step, so they only line up
    // with the trace's intensity changes while it matches the sampling period.
    if (variant_options->HasMember("window_step_seconds"))
        return get_required_positive_double_option(variant_options, "window_step_seconds");

    double sampling_period = _intensity->sampling_period(_signal_property);
    PPK_ASSERT_ERROR(sampling_period > 0.0 && std::isfinite(sampling_period),
        "Missing window_step_seconds: got sampling period %g, expected a finite positive regular CSV period",
        sampling_period);
    return sampling_period;
}

GreenWindowScheduling::~GreenWindowScheduling()
{
}

std::string GreenWindowScheduling::get_required_string_option(
    rapidjson::Document *variant_options, const char *option_name)
{
    PPK_ASSERT_ERROR(
        variant_options->HasMember(option_name), "Invalid options: required member '%s' cannot be found", option_name);
    PPK_ASSERT_ERROR(
        (*variant_options)[option_name].IsString(), "Invalid options: '%s' should be a string", option_name);

    return (*variant_options)[option_name].GetString();
}

std::string GreenWindowScheduling::get_intensity_trace_option(rapidjson::Document *variant_options)
{
    if (variant_options->HasMember("intensity_trace"))
        return get_required_string_option(variant_options, "intensity_trace");

    return get_required_string_option(variant_options, "typical_intensities_file");
}

std::string GreenWindowScheduling::get_signal_property_option(rapidjson::Document *variant_options)
{
    std::string signal = get_required_string_option(variant_options, "signal");

    if (signal == "carbon")
        return CARBON_INTENSITY_PROPERTY;
    if (signal == "water")
        return WATER_INTENSITY_PROPERTY;

    PPK_ASSERT_ERROR(
        false, "Invalid options: 'signal' should be either 'carbon' or 'water' (got '%s')", signal.c_str());
    return "";
}

double GreenWindowScheduling::get_required_positive_double_option(
    rapidjson::Document *variant_options, const char *option_name)
{
    PPK_ASSERT_ERROR(
        variant_options->HasMember(option_name), "Invalid options: required member '%s' cannot be found", option_name);
    PPK_ASSERT_ERROR(
        (*variant_options)[option_name].IsNumber(), "Invalid options: '%s' should be a number", option_name);

    double value = (*variant_options)[option_name].GetDouble();
    PPK_ASSERT_ERROR(value > 0.0, "Invalid options: '%s' should be strictly positive (got %g)", option_name, value);
    return value;
}

double GreenWindowScheduling::get_optional_positive_double_option(
    rapidjson::Document *variant_options, const char *option_name, double default_value)
{
    if (!variant_options->HasMember(option_name))
        return default_value;

    return get_required_positive_double_option(variant_options, option_name);
}

bool GreenWindowScheduling::get_optional_bool_option(
    rapidjson::Document *variant_options, const char *option_name, bool default_value)
{
    if (!variant_options->HasMember(option_name))
        return default_value;

    PPK_ASSERT_ERROR(
        (*variant_options)[option_name].IsBool(), "Invalid options: '%s' should be a boolean", option_name);
    return (*variant_options)[option_name].GetBool();
}

// Optional, 0 when absent. Unlike the other numeric options, 0 is valid: it keeps the original behavior.
double GreenWindowScheduling::get_min_relative_gain_option(rapidjson::Document *variant_options)
{
    const char *option_name = "min_relative_gain";
    if (!variant_options->HasMember(option_name))
        return 0.0;

    PPK_ASSERT_ERROR(
        (*variant_options)[option_name].IsNumber(), "Invalid options: '%s' should be a number", option_name);

    double value = (*variant_options)[option_name].GetDouble();
    PPK_ASSERT_ERROR(
        value >= 0.0 && value < 1.0, "Invalid options: '%s' should be in [0, 1) (got %g)", option_name, value);
    return value;
}

// Optional, "backfill" when absent, which keeps the original behavior.
GreenWindowScheduling::HeadsBehindDisplacement GreenWindowScheduling::get_heads_behind_displacement_option(
    rapidjson::Document *variant_options)
{
    const char *option_name = "heads_behind_displacement";
    if (!variant_options->HasMember(option_name))
        return HeadsBehindDisplacement::Backfill;

    std::string value = get_required_string_option(variant_options, option_name);
    if (value == "backfill")
        return HeadsBehindDisplacement::Backfill;
    if (value == "easy")
        return HeadsBehindDisplacement::Easy;
    if (value == "score")
        return HeadsBehindDisplacement::Score;

    PPK_ASSERT_ERROR(false, "Invalid options: '%s' should be 'backfill', 'easy' or 'score' (got '%s')", option_name,
        value.c_str());
    return HeadsBehindDisplacement::Backfill;
}

void GreenWindowScheduling::log_window_choice(const Job *job, Rational date, Rational t0, const WindowCandidate &now,
    const WindowCandidate &lowest, const WindowCandidate &chosen) const
{
    if (!_green_window_scheduling_debug)
        return;

    rapidjson::Document record(rapidjson::kObjectType);
    rapidjson::Document::AllocatorType &allocator = record.GetAllocator();

    record.AddMember("event", "green_window_selected", allocator);
    record.AddMember("job", rapidjson::Value(job->id.c_str(), allocator), allocator);
    
    record.AddMember("date", (double)date, allocator);
    record.AddMember("t0", (double)t0, allocator);
    record.AddMember("begin", (double)chosen.begin, allocator);
    record.AddMember("end", (double)chosen.end, allocator);

    record.AddMember("impact", chosen.score, allocator);
    // now_impact is the score of starting at date, absent when that start is infeasible.
    if (now.found)
        record.AddMember("now_impact", now.score, allocator);
    
    // The lowest-score window, present only when the minimum gain kept the job at its immediate start.
    if (chosen.begin != lowest.begin)
    {
        record.AddMember("rejected_begin", (double)lowest.begin, allocator);
        record.AddMember("rejected_impact", lowest.score, allocator);
    }

    record.AddMember("machines", rapidjson::Value(chosen.machines.to_string_brackets().c_str(), allocator), allocator);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    record.Accept(writer);
    LOG_F(INFO, "%s", buffer.GetString());
}
