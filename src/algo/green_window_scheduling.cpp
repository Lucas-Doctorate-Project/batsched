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
    execute_displaced_jobs_due(date);
    enqueue_released_jobs(date, update_info);
    _queue->sort_queue(update_info, compare_info);
    schedule_runnable_heads(date);
    backfill_waiting_jobs(date);
    request_displaced_start_calls(date);
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

    if (has_pending_displacement() && _heads_behind_displacement != HeadsBehindDisplacement::Backfill)
        schedule_heads_behind_displacement(date);
}

// EASY for the jobs queued behind the displaced ones: they are taken in queue
// order while they fit, and the first one that does not fit keeps a
// reservation, so the backfill that follows cannot delay it. In the easy mode
// a job that fits starts at once. In the score mode it is scored like the queue
// head, so it may be displaced as well.
void GreenWindowScheduling::schedule_heads_behind_displacement(Rational date)
{
    std::list<SortableJob *>::const_iterator job_it = _queue->begin();

    while (job_it != _queue->end())
    {
        // Advance first: starting the job removes it from the queue.
        const Job *job = (*job_it++)->job;
        if (is_displaced(job))
            continue;

        if (!fits_immediately(job, date))
        {
            reserve_earliest_start_around_holds(job, date);
            return;
        }

        if (_heads_behind_displacement == HeadsBehindDisplacement::Score)
            schedule_at_scored_window(job, date);
        else
            try_start_immediately(job, date);
    }
}

// Held nodes stay idle until their displaced start (see find_exact_allocation).
// A reservation on them before that start would not protect the job, because it
// could not start there. Each displaced start releases its nodes, so the held
// set only changes at those starts: the phases between them are tried in order,
// each avoiding the nodes still held when it begins. After the last start no
// node is held, so the last attempt always succeeds.
void GreenWindowScheduling::reserve_earliest_start_around_holds(const Job *job, Rational date)
{
    std::vector<Rational> boundaries = hold_phase_boundaries(date);
    for (std::size_t phase = 0; phase + 1 < boundaries.size(); ++phase)
    {
        if (try_reserve_before(job, boundaries[phase], boundaries[phase + 1]))
            return;
    }
    _schedule.add_job_first_fit_after_time(job, boundaries.back(), _selector);
    _priority_reservation = job;
}

// Reserve the job's earliest start from `after` that avoids the nodes held at
// `after`, if that start comes before `before`.
bool GreenWindowScheduling::try_reserve_before(const Job *job, Rational after, Rational before)
{
    IntervalSet outside_holds;
    outside_holds.insert(IntervalSet::ClosedInterval(0, _nb_machines - 1));
    outside_holds -= machines_held_at(after);
    LimitedRangeResourceSelector outside_holds_selector(outside_holds);

    const bool assert_insertion_successful = false;
    Schedule::JobAlloc alloc
        = _schedule.add_job_first_fit_after_time(job, after, &outside_holds_selector, assert_insertion_successful);
    if (alloc.has_been_inserted && alloc.begin < before)
    {
        _priority_reservation = job;
        return true;
    }
    _schedule.remove_job_if_exists(job);
    return false;
}

// The date, then every distinct displaced start after it, in increasing order.
std::vector<Rational> GreenWindowScheduling::hold_phase_boundaries(Rational date) const
{
    std::set<Rational> later_starts;
    for (const Displacement &displacement : _displacements)
    {
        if (displacement.start > date)
            later_starts.insert(displacement.start);
    }
    std::vector<Rational> boundaries = { date };
    boundaries.insert(boundaries.end(), later_starts.begin(), later_starts.end());
    return boundaries;
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
    schedule_at_scored_window(job, date);
}

// Start the job now or hold it for a later window, whichever the score picks.
// The job must fit at date.
void GreenWindowScheduling::schedule_at_scored_window(const Job *job, Rational date)
{
    WindowCandidate candidate = find_best_window(job, date);
    PPK_ASSERT_ERROR(candidate.found, "Job '%s' fits at %g, expected a feasible window", job->id.c_str(), (double)date);
    Schedule::JobAlloc alloc = insert_at_scored_window(job, candidate);
    if (alloc.begin == date)
        start_job(job, alloc.used_machines, date);
    else
        hold_displacement(job, alloc);
}

void GreenWindowScheduling::backfill_waiting_jobs(Rational date)
{
    // Revisit the full queue on callbacks as well as releases and completions.
    std::list<SortableJob *>::const_iterator job_it = _queue->begin();
    while (job_it != _queue->end())
    {
        const Job *job = (*job_it++)->job;
        if (!is_displaced(job) && job != _priority_reservation)
            try_start_immediately(job, date);
    }
}

// Starts the job at date if it fits there for its whole walltime, outside the held nodes.
bool GreenWindowScheduling::try_start_immediately(const Job *job, Rational date)
{
    WindowCandidate candidate;
    candidate.begin = date;
    candidate.end = date + job->walltime;
    if (!find_exact_allocation(job, candidate.begin, candidate.end, candidate.machines))
        return false;
    Schedule::JobAlloc alloc = insert_at_scored_window(job, candidate);
    start_job(job, alloc.used_machines, date);
    return true;
}

// Whether the job fits at date for its whole walltime, outside the held nodes.
bool GreenWindowScheduling::fits_immediately(const Job *job, Rational date) const
{
    IntervalSet machines;
    return find_exact_allocation(job, date, date + job->walltime, machines);
}

void GreenWindowScheduling::update_schedule_present(Rational date)
{
    PPK_ASSERT_ERROR(_schedule.nb_slices() > 0);

    if (date < _schedule.begin()->end)
        _schedule.update_first_slice(date);
    else
        _schedule.update_first_slice_removing_remaining_jobs(date);
}

void GreenWindowScheduling::execute_displaced_jobs_due(Rational date)
{
    std::vector<Displacement> still_pending;
    for (const Displacement &displacement : _displacements)
    {
        if (displacement.start <= date)
            start_job(displacement.job, displacement.machines, date);
        else
            still_pending.push_back(displacement);
    }
    _displacements = still_pending;
}

// Displaced nodes remain idle until their start. Other nodes can still backfill.
bool GreenWindowScheduling::has_pending_displacement() const
{
    return !_displacements.empty();
}

bool GreenWindowScheduling::is_displaced(const Job *job) const
{
    for (const Displacement &displacement : _displacements)
    {
        if (displacement.job == job)
            return true;
    }
    return false;
}

// Nodes of the displaced jobs that have not started at `date`.
IntervalSet GreenWindowScheduling::machines_held_at(Rational date) const
{
    IntervalSet held;
    for (const Displacement &displacement : _displacements)
    {
        if (date < displacement.start)
            held += displacement.machines;
    }
    return held;
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

void GreenWindowScheduling::hold_displacement(const Job *job, const Schedule::JobAlloc &alloc)
{
    Displacement displacement;
    displacement.job = job;
    displacement.start = alloc.begin;
    displacement.machines = alloc.used_machines;
    _displacements.push_back(displacement);
}

// One callback per future displaced start, so each displaced job starts on time.
void GreenWindowScheduling::request_displaced_start_calls(Rational date)
{
    for (const Displacement &displacement : _displacements)
    {
        if (displacement.start <= date)
            continue;

        double future_date = (double)displacement.start;
        if (is_call_date_already_requested(future_date))
            continue;

        _decision->add_call_me_later(future_date, (double)date);
        _requested_call_dates.insert(future_date);
    }
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
