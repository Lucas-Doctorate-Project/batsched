// Standalone regression test, with no simulator dependencies:
// c++ -std=c++14 -Isrc/algo test/test_csv_parser.cpp src/algo/csv_parser.cpp -o /tmp/test_csv_parser
// /tmp/test_csv_parser /tmp/test_csv_parser.csv
#include "csv_parser.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <stdexcept>
#include <vector>

static size_t comparisons = 0;
static uint64_t fingerprint = 14695981039346656037ULL;

static void check(bool condition, const std::string & message)
{
    if (!condition)
        throw std::runtime_error(message);
}

// Independent block-overlap oracle, including the existing endpoint extension.
static double overlap_sum(const std::vector<double> & times, const std::vector<double> & values,
                          double start, double end)
{
    if (start > end) std::swap(start, end);
    double sum = 0;
    if (start < times.front())
        sum += values.front() * (std::min(end, times.front()) - start);
    for (size_t i = 0; i + 1 < times.size(); ++i)
        sum += values[i] * std::max(0.0, std::min(end, times[i + 1]) - std::max(start, times[i]));
    if (end > times.back())
        sum += values.back() * (end - std::max(start, times.back()));
    return sum;
}

static void compare(const CSV_Parser & parser, const std::vector<double> & times,
                    const std::vector<double> & values, double start, double end)
{
    double expected = overlap_sum(times, values, start, end);
    double actual = parser.get_sum("carbon_intensity", start, end);
    check(std::abs(actual - expected) <= 1e-9 * std::max(1.0, std::abs(expected)),
          "integral disagrees with block-overlap oracle");
    uint64_t bits;
    static_assert(sizeof(bits) == sizeof(actual), "unexpected double size");
    std::memcpy(&bits, &actual, sizeof(bits));
    fingerprint = (fingerprint ^ bits) * 1099511628211ULL;
    ++comparisons;
}

static void test_series(const std::string & path, const std::vector<double> & times,
                        const std::vector<double> & values, double expected_period)
{
    {
        std::ofstream out(path);
        out << std::setprecision(17) << "timestamp,zone,property,value\n";
        // Input order must not matter, and other zones/properties must stay separate.
        out << times.front() << ",OTHER,carbon_intensity,999\n";
        for (size_t i = times.size(); i-- > 0; )
        {
            out << times[i] << ",TEST,carbon_intensity," << values[i] << '\n';
            out << times[i] << ",TEST,water_intensity,7\n";
        }
    }
    CSV_Parser parser(path, "TEST");
    double period = parser.get_sampling_period("carbon_intensity");
    check(std::isnan(expected_period) ? std::isnan(period) : period == expected_period,
          "sampling period changed");
    check(std::isnan(parser.get_sum("missing", 0, 1)), "missing property must yield NaN");
    check(parser.get_sum("water_intensity", 0, 10) == 70, "wrong property selected");
    check(parser.get_min("carbon_intensity") == *std::min_element(values.begin(), values.end()),
          "minimum changed");
    check(parser.get_max("carbon_intensity") == *std::max_element(values.begin(), values.end()),
          "maximum changed");

    for (double time : times)
    {
        compare(parser, times, values, time, time);
        compare(parser, times, values, times.front(), time);
        compare(parser, times, values, std::nextafter(time, -INFINITY), std::nextafter(time, INFINITY));
    }
    // Check partial blocks, reversed intervals and both extrapolation boundaries.
    double span = std::max(times.back() - times.front(), 1.0);
    compare(parser, times, values, times.front() - span, times.back() + span);
    std::mt19937 random(29);
    std::uniform_real_distribution<double> distribution(times.front() - span, times.back() + span);
    for (int i = 0; i < 2000; ++i)
    {
        double start = distribution(random);
        double end = distribution(random);
        compare(parser, times, values, start, end);
    }
}

int main(int argc, char ** argv)
{
    if (argc != 2)
    {
        std::cerr << "Usage: test_csv_parser <temporary-csv-path>\n";
        return 1;
    }
    const double nan = std::numeric_limits<double>::quiet_NaN();
    try
    {
        test_series(argv[1], {0, 900, 1800, 2700, 3600}, {100, 10, 40, 0, 70}, 900);
        test_series(argv[1], {100, 1000, 1900, 2800, 3700}, {100, 10, 40, 0, 70}, 900);
        test_series(argv[1], {1700000000, 1700000900, 1700001800}, {10, 20, 30}, 900);
        test_series(argv[1], {0, 0.1, 0.2, 0.3, 0.4}, {100, 10, 40, 0, 70}, 0.1);
        test_series(argv[1], {0, 1, 2.0000000005, 3.0000000005}, {10, 20, 30, 40}, 1);
        test_series(argv[1], {100, 107, 133.2, 139.8, 260}, {100, 10, 40, 0, 70}, nan);
        // The historical 1e-9 sampling tolerance accepts these tiny intervals,
        // but arithmetic indexing must fall back once their grid drift is large.
        test_series(argv[1], {0, 1e-10, 3e-10, 9e-10}, {10, 20, 30, 40}, 1e-10);
        test_series(argv[1], {100}, {17}, nan);
        {
            std::ofstream out(argv[1]);
            out << "timestamp,zone,property,value\n";
        }
        CSV_Parser empty(argv[1], "TEST");
        check(std::isnan(empty.get_sum("carbon_intensity", 0, 0)), "empty trace must yield NaN");
        std::remove(argv[1]);
        std::cout << comparisons << " integral comparisons passed, fingerprint "
                  << std::hex << fingerprint << '\n';
    }
    catch (const std::exception & error)
    {
        std::remove(argv[1]);
        std::cerr << error.what() << '\n';
        return 1;
    }
}
