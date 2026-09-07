#pragma once

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

class CSV_Parser
{
public:
    CSV_Parser(const std::string & file, const std::string & zone);
    ~CSV_Parser();

    double get_value(double timestamp, const std::string & property) const;

    double get_sum(const std::string & property, double start, double end) const;
    double get_sampling_period(const std::string & property) const;

    double get_max(const std::string & property) const;
    double get_max(const std::string & property, double start, double end) const;

    double get_min(const std::string & property) const;
    double get_min(const std::string & property, double start, double end) const;

private:
    struct IntegralSeries
    {
        std::vector<double> timestamps;
        std::vector<double> values;
        std::vector<double> prefix;
        double index_period = 0.0;
    };

    std::string _filename;
    std::string _zone;
    std::map<std::string, std::map<double, double>> _series;
    std::unordered_map<std::string, IntegralSeries> _integrals;
    std::map<std::string, double> _sampling_periods;

    static void trim_cr(std::string & s);
    static void strip_quotes(std::string & s);
    void parse_csv();
    void build_integrals();
    static double integral_until(const IntegralSeries & series, double timestamp);
};
