#pragma once

#include <map>
#include <string>
#include <utility> // std::pair

class CSV_Parser
{
public:
    explicit CSV_Parser(const std::string & file);
    ~CSV_Parser();

    std::pair<double, double> get_intensities(double timestamp) const;

    double get_max_carbon_intensity() const;
    double get_max_carbon_intensity(double start, double end) const;

    double get_max_water_intensity() const;
    double get_max_water_intensity(double start, double end) const;

    double get_min_carbon_intensity() const;
    double get_min_carbon_intensity(double start, double end) const;

    double get_min_water_intensity() const;
    double get_min_water_intensity(double start, double end) const;

private:
    std::string _filename;
    double _mx_carbon = 0.0;
    double _mx_water = 0.0;
    double _mn_carbon = 0.0;
    double _mn_water = 0.0;
    std::map<int, std::pair<double, double>> _data;

    static void trim_cr(std::string & s);
    void parse_csv();
};
