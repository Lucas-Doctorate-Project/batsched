#include "green_window_scheduling.hpp"

#include <cmath>

#include "../pempek_assert.hpp"

void GreenWindowScheduling::on_requested_call(double date)
{
    ISchedulingAlgorithm::on_requested_call(date);
    forget_requested_call_date(date);
}

void GreenWindowScheduling::make_decisions(
    double date, SortableJobOrder::UpdateInformation *update_info, SortableJobOrder::CompareInformation *compare_info)
{
    clear_priority_reservation();
    release_completed_allocations();
    update_schedule_present(date);
    execute_reserved_job_if_ready(date);
    enqueue_released_jobs(date, update_info);
    _queue->sort_queue(update_info, compare_info);
    schedule_runnable_heads(date);
    backfill_waiting_jobs(date);
    request_reservation_call(date);
}

void GreenWindowScheduling::release_completed_allocations()
{
    for (const std::string &ended_job_id : _jobs_ended_recently)
        _schedule.remove_job_if_exists((*_workload)[ended_job_id]);
}

void GreenWindowScheduling::enqueue_released_jobs(double date, SortableJobOrder::UpdateInformation *update_info)
{
    for (const std::string &new_job_id : _jobs_released_recently)
    {
        const Job *job = (*_workload)[new_job_id];
        if (job->nb_requested_resources > _nb_machines || !job->has_walltime)
            _decision->add_reject_job(new_job_id, date);
        else
            _queue->append_job(job, update_info);
    }
}

void GreenWindowScheduling::clear_priority_reservation()
{
    if (_priority_reservation == nullptr)
        return;
    _schedule.remove_job_if_exists(_priority_reservation);
    _priority_reservation = nullptr;
}

void GreenWindowScheduling::schedule_runnable_heads(Rational date)
{
    // b46671e dispatched at most one head per event and never traversed backfill jobs.
    while (!has_pending_displacement() && !_queue->is_empty())
    {
        schedule_priority_job(date);
        if (_priority_reservation != nullptr)
            return;
    }
}

void GreenWindowScheduling::schedule_priority_job(Rational date)
{
    const Job *job = _queue->first_job();
    Schedule::JobAlloc earliest = _schedule.add_job_first_fit(job, _selector);
    if (!earliest.started_in_first_slice)
    {
        // Protect the blocked head's EASY start before considering smaller jobs.
        _priority_reservation = job;
        return;
    }
    _schedule.remove_job(job);
    WindowCandidate candidate = find_best_window(job, date);
    PPK_ASSERT_ERROR(candidate.found, "Job '%s' fits at %g, expected a feasible window", job->id.c_str(), (double)date);
    Schedule::JobAlloc alloc = insert_at_scored_window(job, candidate);
    if (alloc.begin == date)
        start_job(job, alloc.used_machines, date);
    else
        hold_displaced_reservation(job, alloc);
}

void GreenWindowScheduling::backfill_waiting_jobs(Rational date)
{
    // Revisit the full queue on callbacks as well as releases and completions.
    std::list<SortableJob *>::const_iterator job_it = _queue->begin();
    while (job_it != _queue->end())
    {
        const Job *job = (*job_it++)->job;
        if (job != _reserved_job && job != _priority_reservation)
            try_backfill_job(job, date);
    }
}

void GreenWindowScheduling::try_backfill_job(const Job *job, Rational date)
{
    WindowCandidate candidate;
    candidate.begin = date;
    candidate.end = date + job->walltime;
    if (!find_exact_allocation(job, candidate.begin, candidate.end, candidate.machines))
        return;
    Schedule::JobAlloc alloc = insert_at_scored_window(job, candidate);
    start_job(job, alloc.used_machines, date);
}

void GreenWindowScheduling::update_schedule_present(Rational date)
{
    PPK_ASSERT_ERROR(_schedule.nb_slices() > 0);

    if (date < _schedule.begin()->end)
        _schedule.update_first_slice(date);
    else
        _schedule.update_first_slice_removing_remaining_jobs(date);
}

void GreenWindowScheduling::execute_reserved_job_if_ready(Rational date)
{
    if (!has_pending_displacement() || date < _reserved_start)
        return;

    start_job(_reserved_job, _reserved_machines, date);
    clear_reservation();
}

// Only the head of the FCFS queue is displaced, with one displacement in flight.
// Its nodes remain idle until the reserved start. Other nodes can still backfill.
bool GreenWindowScheduling::has_pending_displacement() const
{
    return _reserved_job != nullptr;
}

Schedule::JobAlloc GreenWindowScheduling::insert_at_scored_window(const Job *job, const WindowCandidate &candidate)
{
    LimitedRangeResourceSelector exact_selector(candidate.machines);
    Schedule::JobAlloc alloc = _schedule.add_job_first_fit_after_time(job, candidate.begin, &exact_selector);

    PPK_ASSERT_ERROR(alloc.begin == candidate.begin, "Job '%s' was expected to start exactly at %g, but starts at %g",
        job->id.c_str(), (double)candidate.begin, (double)alloc.begin);
    PPK_ASSERT_ERROR(alloc.used_machines == candidate.machines, "Job '%s' was expected to use machines %s, but uses %s",
        job->id.c_str(), candidate.machines.to_string_brackets().c_str(),
        alloc.used_machines.to_string_brackets().c_str());

    return alloc;
}

void GreenWindowScheduling::start_job(const Job *job, const IntervalSet &machines, Rational date)
{
    _decision->add_execute_job(job->id, machines, (double)date);
    _queue->remove_job(job);
    _displacement_origins.erase(job);
}

void GreenWindowScheduling::hold_displaced_reservation(const Job *job, const Schedule::JobAlloc &alloc)
{
    _reserved_job = job;
    _reserved_start = alloc.begin;
    _reserved_machines = alloc.used_machines;
}

void GreenWindowScheduling::clear_reservation()
{
    _reserved_job = nullptr;
    _reserved_start = 0;
    _reserved_machines = IntervalSet::empty_interval_set();
}

void GreenWindowScheduling::request_reservation_call(Rational date)
{
    if (_reserved_job == nullptr || _reserved_start <= date)
        return;

    double future_date = (double)_reserved_start;
    if (is_call_date_already_requested(future_date))
        return;

    _decision->add_call_me_later(future_date, (double)date);
    _requested_call_dates.insert(future_date);
}

void GreenWindowScheduling::forget_requested_call_date(double date)
{
    for (std::set<double>::iterator it = _requested_call_dates.begin(); it != _requested_call_dates.end();)
    {
        if (*it <= date + 1e-9)
            it = _requested_call_dates.erase(it);
        else
            ++it;
    }
}

bool GreenWindowScheduling::is_call_date_already_requested(double date) const
{
    for (double requested_date : _requested_call_dates)
        if (same_date(requested_date, date))
            return true;

    return false;
}

bool GreenWindowScheduling::same_date(double left, double right)
{
    return std::abs(left - right) <= 1e-9;
}
