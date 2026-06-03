#include "csv_parser.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <sstream>
#include <stdexcept>

using namespace std;

CSV_Parser::CSV_Parser(const std::string & file, const std::string & zone) :
    _filename(file),
    _zone(zone)
{
    parse_csv();
}

CSV_Parser::~CSV_Parser()
{
}

void CSV_Parser::trim_cr(std::string & s)
{
    if (!s.empty() && s.back() == '\r')
        s.pop_back();
}

void CSV_Parser::strip_quotes(std::string & s)
{
    if (!s.empty() && s.front() == '"') s.erase(0, 1);
    if (!s.empty() && s.back() == '"') s.pop_back();
}

void CSV_Parser::parse_csv()
{
    ifstream in(_filename);
    if (!in.is_open())
        throw runtime_error("CSV_Parser: cannot open file: " + _filename);

    _series.clear();
    _prefix_integrals.clear();
    _sampling_periods.clear();

    string line;
    if (!getline(in, line))
        return;

    while (getline(in, line))
    {
        trim_cr(line);
        if (line.empty())
            continue;

        stringstream ss(line);
        string ts_s, zone_s, prop_s, val_s;

        if (!getline(ss, ts_s, ',')) continue;
        if (!getline(ss, zone_s, ',')) continue;
        if (!getline(ss, prop_s, ',')) continue;
        if (!getline(ss, val_s, ',')) continue;

        strip_quotes(ts_s);
        strip_quotes(zone_s);
        strip_quotes(prop_s);
        strip_quotes(val_s);

        if (zone_s != _zone)
            continue;

        double ts;
        double val;
        try
        {
            ts = stod(ts_s);
            val = stod(val_s);
        }
        catch (const std::exception &)
        {
            cerr << "CSV_Parser: skipping non-scalar row in " << _filename
                 << " for property '" << prop_s << "' with value '" << val_s << "'\n";
            continue;
        }

        _series[prop_s][ts] = val;
    }

    build_integrals();
}

void CSV_Parser::build_integrals()
{
    _prefix_integrals.clear();
    _sampling_periods.clear();

    for (const auto & series_kv : _series)
    {
        const string & property = series_kv.first;
        const auto & samples = series_kv.second;

        if (samples.empty())
            continue;

        auto previous = samples.begin();
        double cumulative = 0.0;
        _prefix_integrals[property][previous->first] = cumulative;

        bool first_period_found = false;
        bool regular_sampling = true;
        double detected_period = 0.0;

        auto current = previous;
        ++current;
        for (; current != samples.end(); ++current)
        {
            double delta = current->first - previous->first;
            cumulative += previous->second * delta;
            _prefix_integrals[property][current->first] = cumulative;

            if (delta > 0.0)
            {
                if (!first_period_found)
                {
                    detected_period = delta;
                    first_period_found = true;
                }
                else if (std::abs(delta - detected_period) > 1e-9)
                {
                    regular_sampling = false;
                }
            }

            previous = current;
        }

        if (first_period_found && regular_sampling)
            _sampling_periods[property] = detected_period;
    }
}

double CSV_Parser::get_value(double timestamp, const std::string & property) const
{
    auto series_it = _series.find(property);
    if (series_it == _series.end() || series_it->second.empty())
        return std::numeric_limits<double>::quiet_NaN();

    const auto & samples = series_it->second;
    const auto first_it = samples.begin();
    const auto last_it = std::prev(samples.end());

    if (timestamp <= first_it->first)
        return first_it->second;
    if (timestamp >= last_it->first)
        return last_it->second;

    auto it = samples.upper_bound(timestamp);
    --it;
    return it->second;
}

double CSV_Parser::integral_until(const std::string & property, double timestamp) const
{
    auto series_it = _series.find(property);
    if (series_it == _series.end() || series_it->second.empty())
        return std::numeric_limits<double>::quiet_NaN();

    const auto & samples = series_it->second;
    const auto first_it = samples.begin();
    const auto last_it = std::prev(samples.end());

    if (timestamp <= first_it->first)
        return first_it->second * (timestamp - first_it->first);

    if (timestamp >= last_it->first)
    {
        double integral = _prefix_integrals.at(property).at(last_it->first);
        integral += last_it->second * (timestamp - last_it->first);
        return integral;
    }

    auto it = samples.upper_bound(timestamp);
    --it;

    double integral = _prefix_integrals.at(property).at(it->first);
    integral += it->second * (timestamp - it->first);
    return integral;
}

double CSV_Parser::get_sum(const std::string & property, double start, double end) const
{
    if (start > end)
        std::swap(start, end);

    double start_integral = integral_until(property, start);
    double end_integral = integral_until(property, end);

    if (std::isnan(start_integral) || std::isnan(end_integral))
        return std::numeric_limits<double>::quiet_NaN();

    return end_integral - start_integral;
}

double CSV_Parser::get_sampling_period(const std::string & property) const
{
    auto period_it = _sampling_periods.find(property);
    if (period_it == _sampling_periods.end())
        return std::numeric_limits<double>::quiet_NaN();

    return period_it->second;
}

double CSV_Parser::get_max(const std::string & property) const
{
    auto series_it = _series.find(property);
    if (series_it == _series.end() || series_it->second.empty())
        return std::numeric_limits<double>::quiet_NaN();

    const auto & samples = series_it->second;
    double mx = -std::numeric_limits<double>::infinity();
    for (const auto & kv : samples)
        mx = std::max(mx, kv.second);
    return mx;
}

double CSV_Parser::get_max(const std::string & property, double start, double end) const
{
    auto series_it = _series.find(property);
    if (series_it == _series.end() || series_it->second.empty())
        return std::numeric_limits<double>::quiet_NaN();

    if (start > end)
        std::swap(start, end);

    const auto & samples = series_it->second;
    double mx = std::max(get_value(start, property), get_value(end, property));

    auto it = samples.upper_bound(start);
    for (; it != samples.end() && it->first <= end; ++it)
        mx = std::max(mx, it->second);

    return mx;
}

double CSV_Parser::get_min(const std::string & property) const
{
    auto series_it = _series.find(property);
    if (series_it == _series.end() || series_it->second.empty())
        return std::numeric_limits<double>::quiet_NaN();

    const auto & samples = series_it->second;
    double mn = std::numeric_limits<double>::infinity();
    for (const auto & kv : samples)
        mn = std::min(mn, kv.second);
    return mn;
}

double CSV_Parser::get_min(const std::string & property, double start, double end) const
{
    auto series_it = _series.find(property);
    if (series_it == _series.end() || series_it->second.empty())
        return std::numeric_limits<double>::quiet_NaN();

    if (start > end)
        std::swap(start, end);

    const auto & samples = series_it->second;
    double mn = std::min(get_value(start, property), get_value(end, property));

    auto it = samples.upper_bound(start);
    for (; it != samples.end() && it->first <= end; ++it)
        mn = std::min(mn, it->second);

    return mn;
}
