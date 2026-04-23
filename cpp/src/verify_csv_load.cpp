#include "taxi/CsvReader.hpp"

#include <iostream>

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: verify_csv_load <csv_path>\n";
        return 1;
    }

    taxi::CsvReader reader(argv[1]);
    taxi::TripRecord record;
    std::size_t count = 0;
    while (reader.read_next(record)) {
        ++count;
    }

    const auto stats = reader.get_stats();
    std::cout << "records=" << count
              << " rows_read=" << stats.rows_read
              << " parsed_ok=" << stats.rows_parsed_ok
              << " discarded=" << stats.rows_discarded
              << "\n";
    return count > 0 ? 0 : 2;
}
