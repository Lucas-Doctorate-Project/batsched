#include "cats.hpp"
#include "forecast_mode.hpp"

#include <algorithm>
#include <cmath>
#include <loguru.hpp>
#include <stdexcept>

namespace
{
constexpr double day = 86400;
const std::string carbon = "carbon_intensity";

std::string history_file(const rapidjson::Document &options)
{
    if (!options.HasMember("history_file") || !options["history_file"].IsString())
        throw std::invalid_argument("cats requires a history_file with timestamps relative to the workload");
    return options["history_file"].GetString();
}
}

Cats::Cats(Workload *workload, SchedulingDecision *decision, Queue *queue, ResourceSelector *selector,
           double rjms_delay, rapidjson::Document *options)
    : EasyBackfilling(workload, decision, queue, selector, rjms_delay, options),
      _history(history_file(*options), "AS0"), _previous_day(uses_previous_day(*options, true)),
      _step(_history.get_sampling_period(carbon))
{
    if (!std::isfinite(_step) || _step <= 0 || std::fmod(day, _step) != 0)
        throw std::invalid_argument("cats requires regular carbon observations on a grid dividing 24 hours");
    if (!options->HasMember("max_delay_seconds") || !(*options)["max_delay_seconds"].IsUint())
        throw std::invalid_argument("cats requires a nonnegative integer max_delay_seconds");
    _max_delay = (*options)["max_delay_seconds"].GetUint();
}

double Cats::eligible_at(const Job &job) const
{
    const double arrival = job.submission_time;
    const auto name = job.id.substr(job.id.find('!') + 1);
    if (_max_delay == 0 || name.compare(0, 4, "ctx_") == 0 || !job.has_walltime
        || job.nb_requested_resources > _nb_machines)
        return arrival;

    const double walltime = static_cast<double>(job.walltime) - _rjms_delay;
    if (!_previous_day && !_history.covers(carbon, arrival, arrival + _max_delay + walltime))
        throw std::runtime_error("missing future carbon intensity for " + job.id);
    // complete cycles cancel between starts only when the forecast repeats a day
    const double remaining = std::fmod(walltime, day);
    auto score = [&](double candidate) {
        const double cost = _previous_day
            ? _history.previous_day_sum(carbon, arrival, candidate, candidate + remaining)
            : _history.get_sum(carbon, candidate, candidate + walltime);
        if (!std::isfinite(cost))
            throw std::runtime_error("invalid carbon forecast for " + job.id);
        return cost;
    };

    double best = arrival;
    double best_cost = score(best);
    for (double delay = std::min(_step, _max_delay); ; delay = std::min(delay + _step, _max_delay))
    {
        const double cost = score(arrival + delay);
        if (cost < best_cost - 1e-10 * std::max(1.0, std::abs(best_cost)))
        {
            best = arrival + delay;
            best_cost = cost;
        }
        if (delay == _max_delay)
            break;
    }
    return best;
}

void Cats::enqueue_job(const Job *job, SortableJobOrder::UpdateInformation *)
{
    SortableJobOrder::UpdateInformation original(job->submission_time);
    _queue->append_job(job, &original);
}

void Cats::make_decisions(double date, SortableJobOrder::UpdateInformation *update_info,
                          SortableJobOrder::CompareInformation *compare_info)
{
    std::vector<std::string> arrivals;
    arrivals.swap(_jobs_released_recently);
    for (const auto &id : arrivals)
    {
        const Job *job = (*_workload)[id];
        const double eligible = eligible_at(*job);
        LOG_F(INFO, "cats_admission %s %.17g %.17g", id.c_str(), job->submission_time, eligible);
        if (eligible <= date)
            _jobs_released_recently.push_back(id);
        else
        {
            auto &jobs = _pending[eligible];
            if (jobs.empty())
                _decision->add_call_me_later(eligible, date);
            jobs.push_back(id);
        }
    }

    const auto end = _pending.upper_bound(date);
    if (_pending.begin() != end)
    {
        for (auto it = _pending.begin(); it != end; ++it)
            _jobs_released_recently.insert(_jobs_released_recently.end(), it->second.begin(), it->second.end());
        _pending.erase(_pending.begin(), end);
        std::stable_sort(_jobs_released_recently.begin(), _jobs_released_recently.end(),
                         [this](const std::string &a, const std::string &b) {
                             return (*_workload)[a]->submission_time < (*_workload)[b]->submission_time;
                         });
    }
    EasyBackfilling::make_decisions(date, update_info, compare_info);
}
