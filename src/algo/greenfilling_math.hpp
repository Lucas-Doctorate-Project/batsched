#pragma once

#include <algorithm>
#include <cmath>

/**
 * Normalize an environmental intensity value to [tau, 1.0].
 *
 * I' = clamp((I - I_min) / (I_max - I_min), tau, 1.0)
 *
 * When I_max == I_min (division by zero), returns tau.
 * Values below I_min are clamped to tau; values above I_max are clamped to 1.0.
 */
inline double normalize_intensity(double I, double I_min, double I_max, double tau)
{
    if (I_max == I_min)
        return tau;
    double norm = (I - I_min) / (I_max - I_min);
    return std::max(tau, std::min(1.0, norm));
}

/**
 * Compute the number of machines available for backfilling.
 *
 * N_g = floor(N_a * (1 - max(I_c', I_w')))
 *
 * Inversely proportional to environmental intensity: the greener the grid,
 * the more machines are made available for backfilling.
 */
inline int compute_backfill_machines(int N_a,
                                     double I_c, double I_c_min, double I_c_max,
                                     double I_w, double I_w_min, double I_w_max,
                                     double tau)
{
    if (N_a <= 0)
        return 0;
    double Ic_prime = normalize_intensity(I_c, I_c_min, I_c_max, tau);
    double Iw_prime = normalize_intensity(I_w, I_w_min, I_w_max, tau);
    return static_cast<int>(std::floor(N_a * (1.0 - std::max(Ic_prime, Iw_prime))));
}
