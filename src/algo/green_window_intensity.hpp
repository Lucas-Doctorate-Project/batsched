#pragma once

#include "csv_parser.hpp"

class GreenWindowIntensity
{
public:
    virtual ~GreenWindowIntensity() = default;

    /** Integrate a signal for scoring. Example: integral("carbon_intensity", 0, 3600). */
    virtual double integral(const std::string &property, double begin, double end) const = 0;
    /** Supply the trace grid when no step is configured. Example: sampling_period("carbon_intensity"). */
    virtual double sampling_period(const std::string &property) const = 0;
};

class CsvGreenWindowIntensity : public GreenWindowIntensity
{
public:
    /** Isolate CSV input from scheduling. Example: CsvGreenWindowIntensity("trace.csv", "FR"). */
    CsvGreenWindowIntensity(const std::string &filename, const std::string &zone)
        : _csv_parser(filename, zone)
    {
    }

    /** Preserve the parser's integral semantics. Example: integral("water_intensity", 0, 3600). */
    double integral(const std::string &property, double begin, double end) const override
    {
        return _csv_parser.get_sum(property, begin, end);
    }

    /** Preserve the parser's detected grid. Example: sampling_period("water_intensity"). */
    double sampling_period(const std::string &property) const override
    {
        return _csv_parser.get_sampling_period(property);
    }

private:
    CSV_Parser _csv_parser;
};
