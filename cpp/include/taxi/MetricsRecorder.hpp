#pragma once

#include "taxi/BenchmarkRunner.hpp"
#include <string>
#include <vector>
#include <cstddef>

namespace taxi {

struct MetricRow {
    std::string phase;        // e.g. "Phase1_serial" or "Phase2_parallel"
    std::string query;        // e.g. "Q1_time", "Q3_fare"
    std::size_t dataset_size; // number of records loaded
    int         threads;      // OMP thread count (1 = serial)
    RunStats    timing;       // avg/stddev/min/max in ms
    std::size_t matches;      // result set size (0 for aggregations)
    double      extra_val;    // optional extra (e.g. aggregation sum/avg), 0 if unused
};

/**
 * @brief Accumulates benchmark results and serializes them to CSV.
 *
 * CSV columns:
 *   phase, query, dataset_size, threads,
 *   avg_ms, stddev_ms, min_ms, max_ms, runs,
 *   matches, extra_val
 */
class MetricsRecorder {
public:
    void record(const MetricRow& row);

    // Write all recorded rows to a CSV file.
    // Creates parent directories if they do not exist.
    // Throws std::runtime_error if the file cannot be opened.
    void write_csv(const std::string& path) const;

    // Print a human-readable summary table to stdout.
    void print_summary() const;

    bool empty() const { return rows_.empty(); }
    std::size_t size() const { return rows_.size(); }

private:
    std::vector<MetricRow> rows_;
};

} // namespace taxi
