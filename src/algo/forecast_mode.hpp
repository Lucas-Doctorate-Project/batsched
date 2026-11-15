#pragma once

#include <rapidjson/document.h>
#include <stdexcept>
#include <string>

inline bool uses_previous_day(const rapidjson::Document &options, bool default_previous_day)
{
    if (!options.HasMember("forecast"))
        return default_previous_day;
    const auto &value = options["forecast"];
    if (value.IsString())
    {
        const std::string mode = value.GetString();
        if (mode == "previous_day")
            return true;
        if (mode == "known_future")
            return false;
    }
    throw std::invalid_argument("forecast must be previous_day or known_future");
}
