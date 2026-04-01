#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>

#include "algo/greenfilling_math.hpp"

using Catch::Approx;

// ─────────────────────────────────────────────────────────────
// normalize_intensity
// ─────────────────────────────────────────────────────────────

TEST_CASE("normalize_intensity: value at minimum returns tau", "[normalize]")
{
    // (I_min - I_min) / (I_max - I_min) = 0, clamped to tau
    REQUIRE(normalize_intensity(0.0, 0.0, 1.0, 0.1) == Approx(0.1));
    REQUIRE(normalize_intensity(10.0, 10.0, 100.0, 0.2) == Approx(0.2));
}

TEST_CASE("normalize_intensity: value at maximum returns 1.0", "[normalize]")
{
    REQUIRE(normalize_intensity(1.0, 0.0, 1.0, 0.0) == Approx(1.0));
    REQUIRE(normalize_intensity(100.0, 10.0, 100.0, 0.1) == Approx(1.0));
}

TEST_CASE("normalize_intensity: value in the middle returns 0.5 (tau=0)", "[normalize]")
{
    REQUIRE(normalize_intensity(0.5, 0.0, 1.0, 0.0) == Approx(0.5));
    REQUIRE(normalize_intensity(55.0, 10.0, 100.0, 0.0) == Approx(0.5));
}

TEST_CASE("normalize_intensity: I_max == I_min returns tau (div-by-zero guard)", "[normalize]")
{
    REQUIRE(normalize_intensity(5.0, 5.0, 5.0, 0.1) == Approx(0.1));
    REQUIRE(normalize_intensity(0.0, 0.0, 0.0, 0.3) == Approx(0.3));
}

TEST_CASE("normalize_intensity: value below I_min is clamped to tau", "[normalize]")
{
    REQUIRE(normalize_intensity(-1.0, 0.0, 1.0, 0.1) == Approx(0.1));
    REQUIRE(normalize_intensity(5.0, 10.0, 100.0, 0.2) == Approx(0.2));
}

TEST_CASE("normalize_intensity: value above I_max is clamped to 1.0", "[normalize]")
{
    REQUIRE(normalize_intensity(2.0, 0.0, 1.0, 0.0) == Approx(1.0));
    REQUIRE(normalize_intensity(200.0, 10.0, 100.0, 0.0) == Approx(1.0));
}

TEST_CASE("normalize_intensity: tau=0 with value in range", "[normalize]")
{
    REQUIRE(normalize_intensity(0.25, 0.0, 1.0, 0.0) == Approx(0.25));
}

// ─────────────────────────────────────────────────────────────
// compute_backfill_machines
// ─────────────────────────────────────────────────────────────

TEST_CASE("compute_backfill_machines: N_a=0 always returns 0", "[compute]")
{
    REQUIRE(compute_backfill_machines(0, 0.5, 0.0, 1.0, 0.5, 0.0, 1.0, 0.1) == 0);
    REQUIRE(compute_backfill_machines(0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0) == 0);
}

TEST_CASE("compute_backfill_machines: intensities at minimum => nearly all machines", "[compute]")
{
    // I = I_min => I' = tau = 0.1 => N_g = floor(10 * (1 - 0.1)) = floor(9.0) = 9
    REQUIRE(compute_backfill_machines(10, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.1) == 9);
}

TEST_CASE("compute_backfill_machines: intensities at maximum => zero machines", "[compute]")
{
    // I = I_max => I' = 1.0 => N_g = floor(N_a * (1 - 1.0)) = 0
    REQUIRE(compute_backfill_machines(10, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0) == 0);
    REQUIRE(compute_backfill_machines(100, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.1) == 0);
}

TEST_CASE("compute_backfill_machines: intermediate intensity (tau=0)", "[compute]")
{
    // I_c = 0.5 => I_c' = 0.5; I_w = 0.3 => I_w' = 0.3
    // max = 0.5 => N_g = floor(10 * 0.5) = 5
    REQUIRE(compute_backfill_machines(10, 0.5, 0.0, 1.0, 0.3, 0.0, 1.0, 0.0) == 5);
}

TEST_CASE("compute_backfill_machines: carbon dominates water", "[compute]")
{
    // I_c = 0.75 (= 3/4, exact in FP) => I_c' = 0.75; I_w = 0.2 => I_w' = 0.2
    // max = 0.75 => N_g = floor(10 * 0.25) = 2
    REQUIRE(compute_backfill_machines(10, 0.75, 0.0, 1.0, 0.2, 0.0, 1.0, 0.0) == 2);
}

TEST_CASE("compute_backfill_machines: water dominates carbon", "[compute]")
{
    // I_w = 0.875 (= 7/8, exact in FP) => I_w' = 0.875; I_c = 0.2 => I_c' = 0.2
    // max = 0.875 => N_g = floor(10 * 0.125) = 1
    REQUIRE(compute_backfill_machines(10, 0.2, 0.0, 1.0, 0.875, 0.0, 1.0, 0.0) == 1);
}

TEST_CASE("compute_backfill_machines: floor truncates fractional result", "[compute]")
{
    // I_c = 0.3 => I_c' = 0.3; I_w = 0.1 => I_w' = 0.1
    // max = 0.3 => N_g = floor(10 * 0.7) = floor(7.0) = 7
    REQUIRE(compute_backfill_machines(10, 0.3, 0.0, 1.0, 0.1, 0.0, 1.0, 0.0) == 7);

    // I_c = 0.35 => I_c' = 0.35 => N_g = floor(10 * 0.65) = floor(6.5) = 6
    REQUIRE(compute_backfill_machines(10, 0.35, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0) == 6);
}

TEST_CASE("compute_backfill_machines: non-unit range for intensities", "[compute]")
{
    // I_c in [100, 500]: I_c = 300 => norm = (300-100)/(500-100) = 0.5
    // I_w in [0, 10]:   I_w = 0   => norm = 0, clamped to tau = 0.1
    // max(0.5, 0.1) = 0.5 => N_g = floor(20 * 0.5) = 10
    REQUIRE(compute_backfill_machines(20, 300.0, 100.0, 500.0, 0.0, 0.0, 10.0, 0.1) == 10);
}

TEST_CASE("compute_backfill_machines: I_max == I_min uses tau for that metric", "[compute]")
{
    // Both metrics degenerate: I_c_max == I_c_min => I_c' = tau = 0.2
    // I_w = 0.6 => I_w' = 0.6
    // max(0.2, 0.6) = 0.6 => N_g = floor(10 * 0.4) = 4
    REQUIRE(compute_backfill_machines(10, 5.0, 5.0, 5.0, 0.6, 0.0, 1.0, 0.2) == 4);
}
