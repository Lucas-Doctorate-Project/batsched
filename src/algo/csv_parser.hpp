#pragma once

#include <map>
#include <string>

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
    std::string _filename;
    std::string _zone;
    std::map<std::string, std::map<double, double>> _series;
    std::map<std::string, std::map<double, double>> _prefix_integrals;
    std::map<std::string, double> _sampling_periods;

    static void trim_cr(std::string & s);
    static void strip_quotes(std::string & s);
    void parse_csv();
    void build_integrals();
    double integral_until(const std::string & property, double timestamp) const;
};
