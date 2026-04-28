#include "csv_parser.hpp"

#include <cmath>      // std::ceil, std::floor
#include <fstream>    // std::ifstream
#include <limits>     // std::numeric_limits
#include <sstream>    // std::stringstream
#include <stdexcept>  // std::runtime_error

using namespace std;

CSV_Parser::CSV_Parser(const std::string & file) :
    _filename(file)
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

void CSV_Parser::parse_csv()
{
    ifstream in(_filename);
    if (!in.is_open())
        throw runtime_error("CSV_Parser: cannot open file: " + _filename);

    _data.clear();
    _mx_carbon = _mx_water = -numeric_limits<double>::infinity();
    _mn_carbon = _mn_water = numeric_limits<double>::infinity();

    string line;

    // Always skip header line
    if (!getline(in, line))
    {
        _mx_carbon = _mx_water = _mn_carbon = _mn_water = 0.0;
        return; // empty file
    }

    while (getline(in, line))
    {
        trim_cr(line);
        if (line.empty())
            continue;

        stringstream ss(line);
        string ts_s, carbon_s, water_s;

        if (!getline(ss, ts_s, ','))
            continue;
        if (!getline(ss, carbon_s, ','))
            continue;
        if (!getline(ss, water_s, ','))
            continue;

        int ts = stoi(ts_s);
        double carbon = stod(carbon_s);
        double water = stod(water_s);

        _data[ts] = {carbon, water};

        if (carbon > _mx_carbon) _mx_carbon = carbon;
        if (carbon < _mn_carbon) _mn_carbon = carbon;
        if (water > _mx_water) _mx_water = water;
        if (water < _mn_water) _mn_water = water;
    }

    // If file had only header (no data), reset mins/maxes to 0.0
    if (_data.empty())
    {
        _mx_carbon = _mx_water = _mn_carbon = _mn_water = 0.0;
    }
}

std::pair<double, double> CSV_Parser::get_intensities(double timestamp) const
{
    if (_data.empty())
        return {0.0, 0.0};

    // Intensity at time t is the latest sample with ts <= t.
    // Clamp to [first,last] for out-of-range timestamps.
    const auto first_it = _data.begin();
    const auto last_it = std::prev(_data.end());

    if (timestamp <= static_cast<double>(first_it->first))
        return first_it->second;
    if (timestamp >= static_cast<double>(last_it->first))
        return last_it->second;

    int key = static_cast<int>(floor(timestamp));
    auto it = _data.upper_bound(key);
    if (it == _data.begin())
        return it->second;

    --it;
    return it->second;
}

double CSV_Parser::get_max_carbon_intensity() const
{
    return _mx_carbon;
}

double CSV_Parser::get_max_carbon_intensity(double start, double end) const
{
    if (_data.empty())
        return 0.0;

    if (start > end)
        std::swap(start, end);

    auto start_int = get_intensities(start);
    auto end_int = get_intensities(end);
    double mx = std::max(start_int.first, end_int.first);

    int start_key = static_cast<int>(ceil(start));
    int end_key = static_cast<int>(floor(end));

    for (auto it = _data.lower_bound(start_key);
         it != _data.end() && it->first <= end_key;
         ++it)
    {
        mx = std::max(mx, it->second.first);
    }

    return mx;
}

double CSV_Parser::get_max_water_intensity() const
{
    return _mx_water;
}

double CSV_Parser::get_max_water_intensity(double start, double end) const
{
    if (_data.empty())
        return 0.0;

    if (start > end)
        std::swap(start, end);

    auto start_int = get_intensities(start);
    auto end_int = get_intensities(end);
    double mx = std::max(start_int.second, end_int.second);

    int start_key = static_cast<int>(ceil(start));
    int end_key = static_cast<int>(floor(end));

    for (auto it = _data.lower_bound(start_key);
         it != _data.end() && it->first <= end_key;
         ++it)
    {
        mx = std::max(mx, it->second.second);
    }

    return mx;
}

double CSV_Parser::get_min_carbon_intensity() const
{
    return _mn_carbon;
}

double CSV_Parser::get_min_carbon_intensity(double start, double end) const
{
    if (_data.empty())
        return 0.0;

    if (start > end)
        std::swap(start, end);

    auto start_int = get_intensities(start);
    auto end_int = get_intensities(end);
    double mn = std::min(start_int.first, end_int.first);

    int start_key = static_cast<int>(ceil(start));
    int end_key = static_cast<int>(floor(end));

    for (auto it = _data.lower_bound(start_key);
         it != _data.end() && it->first <= end_key;
         ++it)
    {
        mn = std::min(mn, it->second.first);
    }

    return mn;
}

double CSV_Parser::get_min_water_intensity() const
{
    return _mn_water;
}

double CSV_Parser::get_min_water_intensity(double start, double end) const
{
    if (_data.empty())
        return 0.0;

    if (start > end)
        std::swap(start, end);

    auto start_int = get_intensities(start);
    auto end_int = get_intensities(end);
    double mn = std::min(start_int.second, end_int.second);

    int start_key = static_cast<int>(ceil(start));
    int end_key = static_cast<int>(floor(end));

    for (auto it = _data.lower_bound(start_key);
         it != _data.end() && it->first <= end_key;
         ++it)
    {
        mn = std::min(mn, it->second.second);
    }

    return mn;
}