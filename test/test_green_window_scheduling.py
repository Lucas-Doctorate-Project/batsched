#!/usr/bin/env python3
"""Deterministic scenarios for the green_window_scheduling variant.

The platform and workloads live in `green_window/` subdirectories so that
conftest.py's `platforms/*.xml` and `workloads/*.json` globs keep parametrizing
the generic test matrix with the generic fixtures only.

Shared by every scenario below:

- Platform `platforms/green_window/green2hosts.xml`: 2 compute hosts, 10W idle
  and 320W computing, which are also the variant's default power values.
- Jobs declare walltime=3595s and res=2, so each one needs the whole platform
  and they fully serialize. batsched adds --rjms_delay (default 5s) to every
  walltime internally (see json_workload.cpp), so the *effective* walltime used
  to size the windows is 3600s -- exactly one trace sampling period.
- Intensity traces live in `traces/`, use zone TESTZONE, and are sampled every
  3600s. The candidate start dates are therefore t0 plus the multiples of 3600.
"""
import csv
import json
from os.path import abspath

from helper import *


def run_scenario(test_name, planning_horizon_seconds,
                 workload_name='green_window_demo.json',
                 intensity_trace_name='green_window_carbon.csv',
                 signal='carbon',
                 extra_options=None):
    """Run one scenario and return each job's starting time."""
    output_dir, robin_filename, schedconf_filename = init_instance(test_name)

    platform = abspath('platforms/green_window/green2hosts.xml')
    workload = abspath(f'workloads/green_window/{workload_name}')
    intensity_trace = abspath(f'traces/{intensity_trace_name}')
    # This batsim fork requires an environmental-footprint trace regardless of
    # whether the green_window_scheduling variant (a batsched-side, separate CSV)
    # is used; its "zone" column must match a NetZone name from the platform
    # (here, AS0). Its content is otherwise irrelevant to these tests.
    env_footprint_trace = abspath('traces/batsim_env_footprint.csv')

    batcmd = gen_batsim_cmd(platform, workload, output_dir,
        f"--environmental-footprint-dynamic '{env_footprint_trace}'")

    schedconf_content = {
        "intensity_trace": intensity_trace,
        "intensity_zone": "TESTZONE",
        "signal": signal,
        "planning_horizon_seconds": planning_horizon_seconds,
        "green_window_scheduling_debug": True
    }
    if extra_options:
        schedconf_content.update(extra_options)
    write_file(schedconf_filename, json.dumps(schedconf_content))

    instance = RobinInstance(output_dir=output_dir,
        batcmd=batcmd,
        schedcmd=f"batsched -v 'green_window_scheduling' --variant_options_filepath '{schedconf_filename}'",
        simulation_timeout=30, ready_timeout=5,
        success_timeout=10, failure_timeout=0
    )

    instance.to_file(robin_filename)
    ret = run_robin(robin_filename)
    assert ret.returncode == 0

    # helper.py's gen_batsim_cmd passes output_dir itself as batsim's -e
    # (export-prefix) argument, so exported files are named
    # "<output_dir>_<suffix>", not "<output_dir>/<suffix>".
    starting_times = {}
    with open(f'{output_dir}_jobs.csv') as f:
        for row in csv.DictReader(f):
            starting_times[row['job_id']] = float(row['starting_time'])

    return starting_times


def test_displaces_to_cleanest_reachable_window():
    # Carbon trace green_window_carbon.csv: 100 until 7200, then 10, 40 and 70
    # in the three following blocks. Three jobs are submitted at t=0.
    #
    # Only the queue head is displaced, and only one displacement is in flight,
    # so each job searches from its own t0 -- the date its predecessor ends,
    # 3595s after starting:
    #   - job_a is head at t0=0 and reaches the cleanest block, so it waits for
    #     7200 and ends at 10795.
    #   - job_b is head at t0=10795, spanning intensity 40 if it starts now
    #     against 70 for every later candidate, so displacement buys nothing.
    #   - job_c is head at t0=14390, where the trace is flat at 70: all
    #     candidates tie and the earliest wins.
    #
    # The last two therefore also cover the "no gain, do not displace" path and
    # the tie-break toward the earliest start.
    starting_times = run_scenario(
        'green_window_scheduling-green2hosts-green_window_demo',
        planning_horizon_seconds=18000)

    assert starting_times['job_a'] == 7200.0
    assert starting_times['job_b'] == 10795.0
    assert starting_times['job_c'] == 14390.0


def test_starts_immediately_when_no_grid_point_is_reachable():
    # dm bounds where the job may start. With dm=1800s and a 3600s grid, the
    # next grid point after t0=0 is out of reach (3600 > 0+1800), so t0 is the
    # only candidate and the job starts right away -- regardless of its own
    # length, which no longer plays any part in this decision.
    starting_times = run_scenario(
        'green_window_scheduling-green2hosts-green_window_single-short_horizon',
        planning_horizon_seconds=1800,
        workload_name='green_window_single.json')

    assert starting_times['job_single'] == 0.0


def test_window_may_run_past_the_horizon():
    # dm bounds where the job may start, not where it must finish, so a
    # displaced job's execution can extend arbitrarily far past t0+dm.
    #
    # The job is long (effective walltime 10800s, three trace blocks) and dm is
    # only 3600s: horizon_end=3600, and the only reachable candidate besides t0
    # is the grid point at 3600 itself. Displacing there means the whole
    # 10800s run [3600,14400) falls outside [0,3600], four times past the
    # horizon, yet it is still preferred over t0 because it trades the dirty
    # opening block (100) for three cleaner ones (10, 40, 70):
    #
    # t0=0:      exec 100+10+40 over 3600s each, no idle      -> impact 96.0
    # begin=3600: exec 10+40+70 over 3600s each, idle 100*3600 -> impact 78.8
    starting_times = run_scenario(
        'green_window_scheduling-green2hosts-green_window_long',
        planning_horizon_seconds=3600,
        workload_name='green_window_long.json')

    assert starting_times['job_long'] == 3600.0


def test_candidates_align_with_trace_grid():
    # Candidate start dates are anchored on the absolute trace grid rather than
    # stepped from t0, so they land on the intensity breakpoints.
    #
    # The single job is submitted at t=100, off the 3600s grid, and the trace is
    # flat at 100 except for one clean block of intensity 1 covering exactly
    # [7200, 10800) -- the same width as the effective walltime.
    #
    # Grid-anchored candidates include 7200, covering the clean block whole:
    # cost 3600*1. Stepping from t0 would offer 7300 instead, spilling 100s past
    # the block: cost 3500*1 + 100*100, worse by more than an order of
    # magnitude. Asserting 7200 fails if the candidates are re-anchored on t0.
    starting_times = run_scenario(
        'green_window_scheduling-green2hosts-green_window_offgrid',
        planning_horizon_seconds=18000,
        workload_name='green_window_offgrid.json',
        intensity_trace_name='green_window_offgrid_carbon.csv')

    assert starting_times['job_offgrid'] == 7200.0


def test_idle_reservation_outweighs_marginal_gain():
    # Holding the nodes reserved but idle burns energy too, so a window has to
    # be enough cleaner to pay for the wait.
    #
    # Two candidates fit the horizon: t=0 spanning intensity 100, and t=3600
    # spanning 98. Ignoring the reservation, the later one looks 2% cleaner and
    # wins; counting it, idling both nodes for 3600s costs more than the 2%
    # saves, so the job stays at t0.
    #
    # Computing:  320W * 3600s * 100 = 115.20e6  vs  320W * 3600s * 98 = 112.90e6
    # Idle:                        0  vs           10W * 3600s * 100 =    3.60e6
    # Total:               115.20e6  vs                                116.50e6
    starting_times = run_scenario(
        'green_window_scheduling-green2hosts-green_window_marginal',
        planning_horizon_seconds=7200,
        workload_name='green_window_single.json',
        intensity_trace_name='green_window_marginal_carbon.csv')

    assert starting_times['job_single'] == 0.0


def test_cheap_idle_makes_marginal_gain_worth_taking():
    # Same scenario as above with idle_watts lowered from 10 to 1, which is the
    # only difference. The idle term drops to 0.36e6 and no longer covers the
    # 2.30e6 saved by computing in the cleaner block, so the job now displaces.
    #
    # This pins down both that the power options are read and that the idle
    # weighting -- not the intensities alone -- is what settles the trade-off.
    starting_times = run_scenario(
        'green_window_scheduling-green2hosts-green_window_marginal-cheap_idle',
        planning_horizon_seconds=7200,
        workload_name='green_window_single.json',
        intensity_trace_name='green_window_marginal_carbon.csv',
        extra_options={"idle_watts": 1})

    assert starting_times['job_single'] == 3600.0


# Trace green_window_two_signals.csv holds both signals and makes them disagree:
# carbon is clean only in [3600, 7200), water only in [7200, 10800). Each of the
# two tests below therefore fails if the other signal is being read.

def test_carbon_signal_picks_the_carbon_window():
    starting_times = run_scenario(
        'green_window_scheduling-green2hosts-two_signals-carbon',
        planning_horizon_seconds=14400,
        workload_name='green_window_single.json',
        intensity_trace_name='green_window_two_signals.csv',
        signal='carbon')

    assert starting_times['job_single'] == 3600.0


def test_water_signal_picks_the_water_window():
    starting_times = run_scenario(
        'green_window_scheduling-green2hosts-two_signals-water',
        planning_horizon_seconds=14400,
        workload_name='green_window_single.json',
        intensity_trace_name='green_window_two_signals.csv',
        signal='water')

    assert starting_times['job_single'] == 7200.0
