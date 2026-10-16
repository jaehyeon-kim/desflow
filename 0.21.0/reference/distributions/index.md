# Distributions

Every distribution family desflow can draw from, generated from the registry of distributions in the code. [Arrivals and Services](https://jaehyeon.me/desflow/latest/user-guide/arrivals-and-services/index.md) explains how a draw works, and how each family is seeded and changed live.

Each family lists its parameters, the rules they must follow, and the registry path of each parameter that can change during a run. The mean, variance and value with no random generator are computed by the family's own functions for the example given. In a blueprint, a time-valued parameter (`mean`, `std`, `low`, `mode`, `high`, `scale`, `value`, `values`, `edges`) also takes a duration such as `5 min`.

| Family                                  | Parameters                             | Live paths               |
| --------------------------------------- | -------------------------------------- | ------------------------ |
| [`exponential`](#exponential)           | `rate`                                 | `.rate`                  |
| [`normal`](#normal)                     | `mean`, `std`                          | `.mean`, `.std`          |
| [`lognormal`](#lognormal)               | `mean`, `std`                          | `.mean`, `.std`          |
| [`truncated_normal`](#truncated_normal) | `mean`, `std`, `low`                   | `.mean`, `.std`, `.low`  |
| [`constant`](#constant)                 | `value`                                | `.value`                 |
| [`uniform`](#uniform)                   | `low`, `high`                          | `.low`, `.high`          |
| [`triangular`](#triangular)             | `low`, `mode`, `high`                  | `.low`, `.mode`, `.high` |
| [`gamma`](#gamma)                       | `shape`, `scale`                       | `.shape`, `.scale`       |
| [`weibull`](#weibull)                   | `shape`, `scale`                       | `.shape`, `.scale`       |
| [`empirical`](#empirical)               | `values`, `weights`, `edges`, `counts` | none                     |

## exponential

Gaps between independent arrivals; the memoryless distribution.

|                      |                                                                 |
| -------------------- | --------------------------------------------------------------- |
| `dist`               | `exponential`                                                   |
| Parameters           | `rate`                                                          |
| Defaults             | none                                                            |
| Rules                | rate > 0, in events per second                                  |
| Registry paths       | `<sim_id>.service.<name>.rate` (and `.arrival.` for an arrival) |
| Uses a random number | yes                                                             |

For the example `{dist: exponential, rate: 0.5}`: mean 2, variance 4, and with no random generator every draw is 2.

## normal

Symmetric times around a mean. Draws below 0.00001 s (`min_delay`) are raised to it, which raises the mean; truncated_normal avoids that.

|                      |                                                                                                |
| -------------------- | ---------------------------------------------------------------------------------------------- |
| `dist`               | `normal`                                                                                       |
| Parameters           | `mean`, `std`                                                                                  |
| Defaults             | `std` 0                                                                                        |
| Rules                | std >= 0; a std of 0 gives the mean every time                                                 |
| Registry paths       | `<sim_id>.service.<name>.mean`, `<sim_id>.service.<name>.std` (and `.arrival.` for an arrival) |
| Uses a random number | yes                                                                                            |

For the example `{dist: normal, mean: 3.0, std: 0.5}`: mean 3, variance 0.25, and with no random generator every draw is 3.

## lognormal

Positive times with a long right tail, given by their mean and std.

|                      |                                                                                                |
| -------------------- | ---------------------------------------------------------------------------------------------- |
| `dist`               | `lognormal`                                                                                    |
| Parameters           | `mean`, `std`                                                                                  |
| Defaults             | `std` 0                                                                                        |
| Rules                | mean > 0, std >= 0                                                                             |
| Registry paths       | `<sim_id>.service.<name>.mean`, `<sim_id>.service.<name>.std` (and `.arrival.` for an arrival) |
| Uses a random number | yes                                                                                            |

For the example `{dist: lognormal, mean: 4.0, std: 3.0}`: mean 4, variance 9, and with no random generator every draw is 4.

## truncated_normal

A normal that draws again below low instead of clamping.

|                      |                                                                                                                               |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `dist`               | `truncated_normal`                                                                                                            |
| Parameters           | `mean`, `std`, `low`                                                                                                          |
| Defaults             | `std` 0, `low` 0                                                                                                              |
| Rules                | std >= 0; low is 0 when not given; with std 0, mean >= low                                                                    |
| Registry paths       | `<sim_id>.service.<name>.mean`, `<sim_id>.service.<name>.std`, `<sim_id>.service.<name>.low` (and `.arrival.` for an arrival) |
| Uses a random number | yes                                                                                                                           |

For the example `{dist: truncated_normal, mean: 1.0, std: 1.0, low: 0.0}`: mean 1.288, variance 0.6297, and with no random generator every draw is 1.

## constant

The same time every draw, such as a conveyor or a set-up.

|                      |                                                                  |
| -------------------- | ---------------------------------------------------------------- |
| `dist`               | `constant`                                                       |
| Parameters           | `value`                                                          |
| Defaults             | none                                                             |
| Rules                | value >= 0                                                       |
| Registry paths       | `<sim_id>.service.<name>.value` (and `.arrival.` for an arrival) |
| Uses a random number | no                                                               |

For the example `{dist: constant, value: 1.5}`: mean 1.5, variance 0, and with no random generator every draw is 1.5.

## uniform

Every time between low and high equally likely.

|                      |                                                                                                |
| -------------------- | ---------------------------------------------------------------------------------------------- |
| `dist`               | `uniform`                                                                                      |
| Parameters           | `low`, `high`                                                                                  |
| Defaults             | none                                                                                           |
| Rules                | low < high                                                                                     |
| Registry paths       | `<sim_id>.service.<name>.low`, `<sim_id>.service.<name>.high` (and `.arrival.` for an arrival) |
| Uses a random number | yes                                                                                            |

For the example `{dist: uniform, low: 1.0, high: 2.0}`: mean 1.5, variance 0.08333, and with no random generator every draw is 1.5.

## triangular

An expert's estimate: the shortest, the most likely and the longest time.

|                      |                                                                                                                                |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `dist`               | `triangular`                                                                                                                   |
| Parameters           | `low`, `mode`, `high`                                                                                                          |
| Defaults             | none                                                                                                                           |
| Rules                | low \<= mode \<= high, low < high                                                                                              |
| Registry paths       | `<sim_id>.service.<name>.low`, `<sim_id>.service.<name>.mode`, `<sim_id>.service.<name>.high` (and `.arrival.` for an arrival) |
| Uses a random number | yes                                                                                                                            |

For the example `{dist: triangular, low: 1.0, mode: 1.25, high: 2.25}`: mean 1.5, variance 0.07292, and with no random generator every draw is 1.5.

## gamma

Times less variable than exponential. A whole-number shape is the Erlang distribution: that many exponential steps in a row.

|                      |                                                                                                   |
| -------------------- | ------------------------------------------------------------------------------------------------- |
| `dist`               | `gamma`                                                                                           |
| Parameters           | `shape`, `scale`                                                                                  |
| Defaults             | none                                                                                              |
| Rules                | shape > 0, scale > 0; mean = shape * scale                                                        |
| Registry paths       | `<sim_id>.service.<name>.shape`, `<sim_id>.service.<name>.scale` (and `.arrival.` for an arrival) |
| Uses a random number | yes                                                                                               |

For the example `{dist: gamma, shape: 9.0, scale: 0.1667}`: mean 1.5, variance 0.2501, and with no random generator every draw is 1.5.

## weibull

Time to failure: a shape below 1 fails early, above 1 wears out.

|                      |                                                                                                   |
| -------------------- | ------------------------------------------------------------------------------------------------- |
| `dist`               | `weibull`                                                                                         |
| Parameters           | `shape`, `scale`                                                                                  |
| Defaults             | none                                                                                              |
| Rules                | shape > 0, scale > 0                                                                              |
| Registry paths       | `<sim_id>.service.<name>.shape`, `<sim_id>.service.<name>.scale` (and `.arrival.` for an arrival) |
| Uses a random number | yes                                                                                               |

For the example `{dist: weibull, shape: 1.5, scale: 3600.0}`: mean 3250, variance 4.869e+06, and with no random generator every draw is 3250.

## empirical

Times taken from real data: listed values with optional weights, or a histogram of edges and counts. Taken at load; they do not change live.

|                      |                                                                                                                                        |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `dist`               | `empirical`                                                                                                                            |
| Parameters           | `values`, `weights`, `edges`, `counts`                                                                                                 |
| Defaults             | none                                                                                                                                   |
| Rules                | values, or edges and counts; one weight per value and one count per bin, each 0 or more and adding up to more than 0; edges increasing |
| Registry paths       | none: the lists are taken at load                                                                                                      |
| Uses a random number | yes                                                                                                                                    |

For the example `{dist: empirical, values: [1.2, 1.5, 1.5, 2.1, 3.0]}`: mean 1.86, variance 0.4104, and with no random generator every draw is 1.86.
