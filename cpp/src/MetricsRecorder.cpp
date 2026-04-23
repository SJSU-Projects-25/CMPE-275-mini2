#include "taxi/MetricsRecorder.hpp"
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <filesystem>

namespace taxi {

void MetricsRecorder::record(const MetricRow& row) {
    rows_.push_back(row);
}

void MetricsRecorder::write_csv(const std::string& path) const {
    // Create parent directory if needed
    std::filesystem::path p(path);
    if (p.has_parent_path()) {
        std::filesystem::create_directories(p.parent_path());
    }

    std::ofstream out(path);
    if (!out.is_open()) {
        throw std::runtime_error("MetricsRecorder: cannot open output file: " + path);
    }

    out << "phase,query,dataset_size,threads,"
           "avg_ms,stddev_ms,min_ms,max_ms,runs,"
           "matches,extra_val\n";

    out << std::fixed << std::setprecision(4);
    for (const auto& r : rows_) {
        out << r.phase        << ','
            << r.query        << ','
            << r.dataset_size << ','
            << r.threads      << ','
            << r.timing.avg_ms    << ','
            << r.timing.stddev_ms << ','
            << r.timing.min_ms    << ','
            << r.timing.max_ms    << ','
            << r.timing.runs      << ','
            << r.matches          << ','
            << r.extra_val        << '\n';
    }
}

void MetricsRecorder::print_summary() const {
    if (rows_.empty()) {
        std::cout << "(no metrics recorded)\n";
        return;
    }

    // Column widths
    const int W_PHASE = 18, W_QUERY = 16, W_THR = 8,
              W_AVG = 10, W_STD = 10, W_MIN = 8, W_MAX = 8, W_MATCHES = 10;

    auto sep = [&]() {
        std::cout << std::string(W_PHASE + W_QUERY + W_THR +
                                 W_AVG + W_STD + W_MIN + W_MAX + W_MATCHES + 9*3, '-')
                  << '\n';
    };

    sep();
    std::cout << std::left
              << std::setw(W_PHASE)   << "Phase"
              << std::setw(W_QUERY)   << "Query"
              << std::setw(W_THR)     << "Threads"
              << std::right
              << std::setw(W_AVG)     << "Avg(ms)"
              << std::setw(W_STD)     << "Std(ms)"
              << std::setw(W_MIN)     << "Min(ms)"
              << std::setw(W_MAX)     << "Max(ms)"
              << std::setw(W_MATCHES) << "Matches"
              << '\n';
    sep();

    std::cout << std::fixed << std::setprecision(3);
    for (const auto& r : rows_) {
        std::cout << std::left
                  << std::setw(W_PHASE)   << r.phase
                  << std::setw(W_QUERY)   << r.query
                  << std::setw(W_THR)     << r.threads
                  << std::right
                  << std::setw(W_AVG)     << r.timing.avg_ms
                  << std::setw(W_STD)     << r.timing.stddev_ms
                  << std::setw(W_MIN)     << r.timing.min_ms
                  << std::setw(W_MAX)     << r.timing.max_ms
                  << std::setw(W_MATCHES) << r.matches
                  << '\n';
    }
    sep();
}

} // namespace taxi
