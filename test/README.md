### Fast scheduler regressions

From the repository root, after the usual `meson setup build`, run:

```sh
meson test -C build --print-errorlogs
```

With the repository's Nix development environment:

```sh
nix develop --offline -c meson test -C build --print-errorlogs
```

This builds and runs the C++ green-window scenarios and the CSV integral oracle. The scheduler scenarios inject `FakeGreenWindowIntensity` and use in-memory workload and protocol objects. They require no Batsim, Redis, network, or trace files. They cover dispatching multiple heads, EASY backfill protection, idle holds on displaced nodes, arrivals and early completions, callbacks, fixed displacement deadlines, and runnable-time origins. The CSV oracle uses a temporary file in the build directory.

`planning_horizon_seconds` is the maximum displacement from the first time a queue head can run, not from submission. Reconsidering that job retains its original deadline. Only one head is displaced at a time. Its selected nodes stay idle until its reserved start, while other nodes can backfill. A head blocked by running jobs retains an EASY start reservation so backfills cannot delay it.

### Batsim integration tests
``` bash
nix-shell ../release.nix -A integration_tests --command 'pytest'
# or just pytest, but you must prepare your env...
```

Optionally, use Batsim's binary cache to avoid recompiling many things (e.g., SimGrid).

``` bash
nix-env -iA cachix -f https://cachix.org/api/v1/install # installs cachix
cachix use batsim # add Batsim's cachix storage as a Nix remote cache
```

### How does it work?
0. nix-shell puts you into an environment where batsched, batsim, robin, redis, etc. are available (code in [release.nix])
1. pytest generates combinations of test input (code in [conftest.py])
2. for each combination of inputs: (code in [test_runner.py])
  1. pytest generates a [robin] input file
  2. pytest generates batsim and batsched input files if needed
  3. pytest executes [robin] or [robintest] on the generated file

### Running a specific test
You can manually rerun a test with robin:
```
robin test-instances/FAILING-TEST.yaml
```

You can also run batsim and batsched in different terminals:
``` bash
# feel free to hack these files — e.g., prepend commands with gdb, valgrind...
./test-out/FAILING-TEST/cmd/batsim.bash
./test-out/FAILING-TEST/cmd/sched.bash
```

[release.nix]: ../release.nix
[conftest.py]: ./conftest.py
[test_runner.py]: ./test_runner.py
[robin]: https://framagit.org/batsim/batexpe
[robintest]: https://framagit.org/batsim/batexpe
