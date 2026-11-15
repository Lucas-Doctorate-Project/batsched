"""CATS admission regressions. Run with batsim and batsched in PATH on the simulation host."""

import csv
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

FIXTURES = Path(__file__).resolve().parent


class CatsAdmissionTest(unittest.TestCase):
    def run_schedule(self, specs, budget=21600, variant="cats", forecast=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            trace = root / "history.csv"
            with trace.open("w") as out:
                out.write("timestamp,zone,property,value\n")
                for t in range(-86400, 86401, 900):
                    clean = t in (-82800, -81900) if t < 0 else t < 1800
                    out.write(f"{t},AS0,carbon_intensity,{10 if clean else 100}\n")
            options = root / "cats.json"
            setup = {"max_delay_seconds": budget, "history_file": str(trace)} if variant == "cats" else {
                "planning_horizon_seconds": budget, "intensity_trace": str(trace),
                "intensity_zone": "AS0", "signal": "carbon",
            }
            if forecast is not None:
                setup["forecast"] = forecast
            options.write_text(json.dumps(setup))
            workload = root / "workload.json"
            workload.write_text(json.dumps({
                "nb_res": 2,
                "jobs": [{"id": name, "res": 2, "walltime": walltime, "subtime": arrival, "profile": name}
                         for name, arrival, walltime, runtime in specs],
                "profiles": {name: {"type": "delay", "delay": runtime}
                             for name, arrival, walltime, runtime in specs},
            }))
            socket = ["--socket-endpoint", f"ipc://{root}/sock"]
            with (root / "log").open("w") as log:
                sched = subprocess.Popen(["batsched", "-v", variant, "--variant_options_filepath", options, *socket],
                                         stdout=log, stderr=log)
                try:
                    subprocess.run(["batsim", "-p", FIXTURES / "platforms/green_window/green2hosts.xml",
                                    "-w", workload, "-e", root / "out", "--environmental-footprint-dynamic",
                                    FIXTURES / "traces/batsim_env_footprint.csv", *socket],
                                   stdout=log, stderr=log, check=True, timeout=30)
                    self.assertEqual(sched.wait(timeout=5), 0)
                finally:
                    if sched.poll() is None:
                        sched.kill()
                        sched.wait()
            with (root / "out_jobs.csv").open() as stream:
                return {j["job_id"]: float(j["starting_time"]) for j in csv.DictReader(stream)}

    def test_causal_forecast_wakes_an_idle_cluster(self):
        self.assertEqual(self.run_schedule([("alone", 1, 900, 100)]), {"alone": 3601})

    def test_information_swap_at_off_grid_arrival(self):
        specs = [("alone", 1, 900, 100)]
        for variant, previous in (("cats", 3601), ("green_window_scheduling", 3600)):
            for forecast, expected in (("previous_day", previous), ("known_future", 1)):
                with self.subTest(variant=variant, forecast=forecast):
                    self.assertEqual(self.run_schedule(specs, variant=variant, forecast=forecast), {"alone": expected})

    def test_zero_budget(self):
        self.assertEqual(self.run_schedule([("alone", 1, 900, 100)], budget=0), {"alone": 1})

    def test_original_fcfs_order_after_admission(self):
        specs = [("ctx_block", 0, 7200, 5400), ("older", 1, 900, 100), ("newer", 3600, 900, 100)]
        self.assertEqual(self.run_schedule(specs), {"ctx_block": 0, "older": 5400, "newer": 5500})


if __name__ == "__main__":
    unittest.main()
