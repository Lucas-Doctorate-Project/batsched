[![pipeline status](https://framagit.org/batsim/batsched/badges/master/pipeline.svg)](https://framagit.org/batsim/batsched/pipelines)
[![coverage](https://img.shields.io/codecov/c/github/oar-team/batsched.svg)](https://codecov.io/gh/oar-team/batsched)
[![changelog](https://img.shields.io/badge/doc-changelog-blue.svg)](./CHANGELOG.md)

**batsched** is a set of [Batsim]-compatible algorithms implemented in C++.

## Install
### For [Nix] users
``` bash
# Up-to-date version
nix-env -iA batsched-master -f 'https://github.com/oar-team/nur-kapack/archive/master.tar.gz'
# Latest release
nix-env -iA batsched -f 'https://github.com/oar-team/nur-kapack/archive/master.tar.gz'
```

### Manually
``` bash
git clone https://framagit.org/batsim/batsched.git
mkdir -p batsched/build
cd batsched/build
cmake ..
make
make install
```

Up-to-date dependencies and versions are fully defined in [batsched's CI nix recipe](./release.nix).  
Here is a quick (and probably outdated) list:
- decent clang/gcc and cmake
- zmq (C and C++)
- redox (hiredis + libev)
- [loguru]
- [intervalset]
- decent boost, gmp, rapidjson...

[Batsim]: https://framagit.org/batsim/batsim/
[intervalset]: https://framagit.org/batsim/intervalset
[loguru]: https://github.com/emilk/loguru
[Nix]: https://nixos.org/nix/

## CATS-based admission with EASY

The `cats` variant scores a job once at submission using its requested walltime. It searches starts spaced by one trace step from arrival, including arrival and the maximum delay. Its forecast repeats the last 24 hours of completed carbon-intensity samples. It selects the earliest minimum integrated intensity and wakes EASY when the job becomes eligible. Before eligibility the job occupies no nodes and holds no reservation. Eligible jobs retain their original submission time for FCFS. Jobs with an ID beginning with `ctx_` enter EASY immediately.

Pass a JSON file with `--variant_options_filepath`. For example:

```json
{"history_file": "carbon-with-history.csv", "max_delay_seconds": 21600}
```

The CSV uses the Batsim trace columns `timestamp,zone,property,value` and the series `AS0,carbon_intensity`. Supply a regular grid dividing 24 hours and include the day before time zero with negative timestamps. Use `max_delay_seconds = 0` to check EASY equivalence. The log records `cats_admission JOB SUBMITTED ELIGIBLE` for each arrival.


## Intensity information in the six-hour comparison

Both `cats` and `green_window_scheduling` accept a `forecast` option. The default for `cats` is `previous_day`. The default for `green_window_scheduling` is `known_future`. Set either explicitly to cross the same policies with both information sources.

With `previous_day`, the scheduler freezes the most recent 24 hours of completed samples at the decision and repeats that day for the entire lookahead, including runs longer than 24 hours. Supply pre-window history in the scheduler's CSV. With `known_future`, score the candidate execution interval against the realized intensity trace. The simulator accounts both modes against the realized trace. CATS issues its forecast at submission, and the head policy issues it when it searches for a start for a runnable head.
