#include "taxi/CsvReader.hpp"
#include <sstream>
#include <stdexcept>
#include <algorithm>
#include <ctime>
#include <cctype>
#include <iomanip>

namespace taxi {

CsvReader::CsvReader(const std::string& filepath)
    : file_(filepath, std::ios::in), stats_(), header_read_(false) {
    if (!file_.is_open()) {
        throw std::runtime_error("Failed to open CSV file: " + filepath);
    }
    // Skip header line
    std::string header;
    if (std::getline(file_, header)) {
        header_read_ = true;
    }
}

CsvReader::~CsvReader() {
    if (file_.is_open()) {
        file_.close();
    }
}

bool CsvReader::is_open() const {
    return file_.is_open() && file_.good();
}

bool CsvReader::read_next(TripRecord& record) {
    std::string line;
    
    // Keep reading until we get a valid record or hit EOF
    while (std::getline(file_, line)) {
        stats_.rows_read++;
        
        // Skip empty/whitespace-only lines
        if (line.empty() || (line.find_first_not_of(" \t\r\n") == std::string::npos)) {
            stats_.rows_discarded++;
            continue; // Try next line
        }
        
        // Try to parse the line
        if (parse_line(line, record)) {
            stats_.rows_parsed_ok++;
            return true; // Successfully parsed a record
        } else {
            stats_.rows_discarded++;
            // Continue to next line even if parsing failed
            continue;
        }
    }
    
    // EOF reached or read error
    return false;
}

bool CsvReader::parse_line(const std::string& line, TripRecord& record) {
    /**
     * Parse CSV line into TripRecord
     * 
     * Expected CSV column order (17 fields):
     * 0: VendorID
     * 1: tpep_pickup_datetime
     * 2: tpep_dropoff_datetime
     * 3: passenger_count
     * 4: trip_distance
     * 5: RatecodeID
     * 6: store_and_fwd_flag
     * 7: PULocationID
     * 8: DOLocationID
     * 9: payment_type
     * 10: fare_amount
     * 11: extra
     * 12: mta_tax
     * 13: tip_amount
     * 14: tolls_amount
     * 15: improvement_surcharge
     * 16: total_amount
     */
    
    try {
        auto tokens = split_csv_line(line);
        
        if (tokens.size() < 17 || tokens.size() > 19) {
            return false;
        }

        // Helper lambda to safely parse integer with default
        auto parse_int = [](const std::string& str, int default_val = 0) -> int {
            if (str.empty() || str.find_first_not_of(" \t") == std::string::npos) {
                return default_val;
            }
            try {
                return std::stoi(str);
            } catch (...) {
                return default_val;
            }
        };

        // Helper lambda to safely parse double with default
        auto parse_double = [](const std::string& str, double default_val = 0.0) -> double {
            if (str.empty() || str.find_first_not_of(" \t") == std::string::npos) {
                return default_val;
            }
            try {
                return std::stod(str);
            } catch (...) {
                return default_val;
            }
        };

        // Parse critical fields - if these fail, discard the row
        std::int64_t pickup_ts = parse_timestamp(tokens[1]);
        std::int64_t dropoff_ts = parse_timestamp(tokens[2]);
        
        if (pickup_ts <= 0 || dropoff_ts <= pickup_ts) {
            return false; // Invalid timestamps - discard row
        }

        // Parse all fields
        record.vendor_id = parse_int(tokens[0], 0);
        record.pickup_timestamp = pickup_ts;
        record.dropoff_timestamp = dropoff_ts;
        record.passenger_count = parse_int(tokens[3], 0);
        record.trip_distance = parse_double(tokens[4], 0.0);
        record.rate_code_id = parse_int(tokens[5], 0);
        
        // Parse store_and_fwd_flag (Y/N -> true/false)
        std::string flag = tokens[6];
        std::transform(flag.begin(), flag.end(), flag.begin(), ::toupper);
        record.store_and_fwd_flag = (flag == "Y" || flag == "YES" || flag == "TRUE" || flag == "1");
        
        record.pu_location_id = parse_int(tokens[7], 0);
        record.do_location_id = parse_int(tokens[8], 0);
        record.payment_type = parse_int(tokens[9], 0);
        
        // Parse monetary fields (normalize empty/missing to 0.0)
        record.fare_amount = parse_double(tokens[10], 0.0);
        record.extra = parse_double(tokens[11], 0.0);
        record.mta_tax = parse_double(tokens[12], 0.0);
        record.tip_amount = parse_double(tokens[13], 0.0);
        record.tolls_amount = parse_double(tokens[14], 0.0);
        record.improvement_surcharge = parse_double(tokens[15], 0.0);
        record.total_amount = parse_double(tokens[16], 0.0);

        // Validate record meets minimum requirements
        if (!record.is_valid()) {
            return false;
        }

        return true;
        
    } catch (const std::exception&) {
        // Any parsing error - discard row
        return false;
    }
}

std::vector<std::string> CsvReader::split_csv_line(const std::string& line) {
    /**
     * CSV Line Parser
     * 
     * Handles RFC 4180-compliant CSV format:
     * - Fields may be quoted with double quotes
     * - Quoted fields may contain commas
     * - Escaped quotes within quoted fields are represented as ""
     * - Empty fields are allowed
     * 
     * Example: "field1","field,with,commas","field""with""quotes",field4
     * Results: ["field1", "field,with,commas", "field\"with\"quotes", "field4"]
     */
    std::vector<std::string> tokens;
    std::string current_token;
    bool in_quotes = false;
    
    for (std::size_t i = 0; i < line.length(); ++i) {
        char c = line[i];
        
        if (c == '"') {
            if (in_quotes && i + 1 < line.length() && line[i + 1] == '"') {
                // Escaped quote (double quote) - add single quote to token
                current_token += '"';
                ++i; // Skip next quote
            } else {
                // Toggle quote state
                in_quotes = !in_quotes;
                // Note: We don't add the quote character itself to the token
            }
        } else if (c == ',' && !in_quotes) {
            // Field separator - save current token and start new one
            tokens.push_back(current_token);
            current_token.clear();
        } else {
            // Regular character - add to current token
            current_token += c;
        }
    }
    
    // Add the last token (after final comma or end of line)
    tokens.push_back(current_token);
    
    return tokens;
}

std::int64_t CsvReader::parse_timestamp(const std::string& timestamp_str) {
    if (timestamp_str.empty()) {
        return 0;
    }

    try {
        int year = 0, month = 0, day = 0, hour = 0, min = 0, sec = 0;
        std::string ampm;

        if (timestamp_str.size() >= 2 && timestamp_str[2] == '/') {
            // Format: "MM/DD/YYYY HH:MM:SS AM/PM"
            char slash1, slash2, colon1, colon2;
            std::istringstream ss(timestamp_str);
            ss >> month >> slash1 >> day >> slash2 >> year
               >> hour >> colon1 >> min >> colon2 >> sec >> ampm;
            if (ss.fail()) return 0;
        } else if (timestamp_str.size() >= 4 &&
                   std::isdigit(static_cast<unsigned char>(timestamp_str[0])) &&
                   std::isdigit(static_cast<unsigned char>(timestamp_str[3]))) {
            // Format: "YYYY MMM DD HH:MM:SS AM/PM"
            std::string month_str;
            char colon1, colon2;
            std::istringstream ss(timestamp_str);
            ss >> year >> month_str >> day >> hour >> colon1 >> min >> colon2 >> sec >> ampm;
            if (ss.fail()) return 0;

            const char* months[] = {"Jan","Feb","Mar","Apr","May","Jun",
                                    "Jul","Aug","Sep","Oct","Nov","Dec"};
            for (int i = 0; i < 12; ++i) {
                if (month_str == months[i]) { month = i + 1; break; }
            }
            if (month == 0) return 0;
        } else {
            // Format: "YYYY-MM-DD HH:MM:SS" (24-hour, no AM/PM)
            std::tm tm = {};
            std::istringstream ss(timestamp_str);
            ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
            if (ss.fail()) return 0;
            year  = tm.tm_year + 1900;
            month = tm.tm_mon + 1;
            day   = tm.tm_mday;
            hour  = tm.tm_hour;
            min   = tm.tm_min;
            sec   = tm.tm_sec;
        }

        // 12-hour to 24-hour conversion when AM/PM is present
        if (!ampm.empty()) {
            std::transform(ampm.begin(), ampm.end(), ampm.begin(), ::toupper);
            if (ampm == "PM" && hour != 12) hour += 12;
            else if (ampm == "AM" && hour == 12) hour = 0;
        }

        // Days since Unix epoch
        std::int64_t days_since_epoch = 0;
        for (int y = 1970; y < year; ++y) {
            bool leap = ((y % 4 == 0 && y % 100 != 0) || (y % 400 == 0));
            days_since_epoch += leap ? 366 : 365;
        }
        int days_in_month[] = {31,28,31,30,31,30,31,31,30,31,30,31};
        if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))
            days_in_month[1] = 29;
        for (int m = 0; m < month - 1; ++m)
            days_since_epoch += days_in_month[m];
        days_since_epoch += day - 1;

        return days_since_epoch * 86400LL
             + static_cast<std::int64_t>(hour) * 3600LL
             + static_cast<std::int64_t>(min)  * 60LL
             + static_cast<std::int64_t>(sec);

    } catch (const std::exception&) {
        return 0;
    }
}

// ---- Parallel chunk loader --------------------------------------------------

std::vector<TripRecord> CsvReader::load_chunk(
    const std::string& path,
    std::int64_t byte_start,
    std::int64_t byte_end,
    Stats& out_stats)
{
    std::ifstream f(path, std::ios::in);
    if (!f.is_open()) {
        throw std::runtime_error("CsvReader::load_chunk: cannot open " + path);
    }

    if (byte_start > 0) {
        // Seek into the middle of the file. The byte boundary almost certainly
        // falls inside a line that started in the previous chunk, so discard
        // everything up to and including the next newline.  The previous chunk
        // will have read that line in full, so ownership is unambiguous.
        f.seekg(byte_start);
        if (!f.good()) {
            throw std::runtime_error(
                "CsvReader::load_chunk: seek failed for " + path);
        }
        std::string discard;
        std::getline(f, discard);
    } else {
        // Chunk 0: skip the CSV header row.
        std::string header;
        std::getline(f, header);
    }

    // Build a bare CsvReader that owns the already-positioned stream.
    CsvReader reader;
    reader.file_ = std::move(f);
    reader.header_read_ = true;

    std::vector<TripRecord> results;
    TripRecord rec;

    while (reader.file_.good()) {
        // Check position BEFORE reading the line so every line is owned by
        // exactly one chunk: lines whose first byte is <= byte_end belong here.
        auto pos = static_cast<std::int64_t>(reader.file_.tellg());
        if (pos < 0 || pos > byte_end) break;

        std::string line;
        if (!std::getline(reader.file_, line)) break;

        reader.stats_.rows_read++;

        if (line.empty() ||
            line.find_first_not_of(" \t\r\n") == std::string::npos) {
            reader.stats_.rows_discarded++;
            continue;
        }

        if (reader.parse_line(line, rec)) {
            reader.stats_.rows_parsed_ok++;
            results.push_back(rec);
        } else {
            reader.stats_.rows_discarded++;
        }
    }

    out_stats = reader.stats_;
    return results;
}

} // namespace taxi
