#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>
#include <nlohmann/json.hpp>

#include "mini2.grpc.pb.h"
#include "mini2.pb.h"

namespace {

constexpr int kGrpcMaxMessageBytes = 1800 * 1024 * 1024;

struct ClientOptions {
    std::filesystem::path config_path = "config/topology.json";
    std::string query_type;
    std::int64_t start_time = 0;
    std::int64_t end_time = 0;
    double num_min = 0.0;
    double num_max = 0.0;
    std::int32_t int_min = 0;
    std::int32_t int_max = 0;
    std::int64_t combined_start = 0;
    std::int64_t combined_end = 0;
    double combined_dist_min = 0.0;
    double combined_dist_max = 0.0;
    std::int32_t combined_pass_min = 0;
    std::int32_t combined_pass_max = 0;
    std::string request_id;
};

void print_usage(const char* argv0) {
    std::cerr
        << "Usage: " << argv0 << " --config <path> --query-type <type> [options]\n"
        << "  --config           Topology JSON (default: config/topology.json)\n"
        << "  --query-type       time | distance | fare | location | combined | aggregate\n"
        << "  --request-id       Optional; server generates if omitted\n"
        << "time / aggregate:\n"
        << "  --start-time <i64>  --end-time <i64>\n"
        << "distance / fare:\n"
        << "  --min <double>  --max <double>\n"
        << "location:\n"
        << "  --min <int32>  --max <int32>\n"
        << "combined:\n"
        << "  --start-time <i64> --end-time <i64>\n"
        << "  --dist-min <double> --dist-max <double>\n"
        << "  --pass-min <int32> --pass-max <int32>\n";
}

std::string resolve_entry_endpoint(const std::filesystem::path& config_path) {
    std::ifstream in(config_path);
    if (!in) {
        throw std::runtime_error("Cannot open config: " + config_path.string());
    }
    nlohmann::json j;
    in >> j;
    const auto& nodes = j.at("nodes");
    for (auto it = nodes.begin(); it != nodes.end(); ++it) {
        const auto& cfg = it.value();
        if (cfg.value("is_entry", false)) {
            std::string host = cfg.at("host").get<std::string>();
            if (host == "0.0.0.0" || host == "::") {
                host = "127.0.0.1";
            }
            const int port = cfg.at("port").get<int>();
            return host + ":" + std::to_string(port);
        }
    }
    throw std::runtime_error("No entry node (is_entry: true) in topology");
}

void fill_query_request(const ClientOptions& opt, mini2::QueryRequest* out) {
    out->set_query_type(opt.query_type);
    if (!opt.request_id.empty()) {
        out->set_request_id(opt.request_id);
    }
    if (opt.query_type == "time" || opt.query_type == "aggregate") {
        out->mutable_time_query()->set_start_time(opt.start_time);
        out->mutable_time_query()->set_end_time(opt.end_time);
    } else if (opt.query_type == "distance" || opt.query_type == "fare") {
        out->mutable_numeric_query()->set_min_val(opt.num_min);
        out->mutable_numeric_query()->set_max_val(opt.num_max);
    } else if (opt.query_type == "location") {
        out->mutable_int_query()->set_min_val(opt.int_min);
        out->mutable_int_query()->set_max_val(opt.int_max);
    } else if (opt.query_type == "combined") {
        out->mutable_combined_query()->mutable_time_range()->set_start_time(opt.combined_start);
        out->mutable_combined_query()->mutable_time_range()->set_end_time(opt.combined_end);
        out->mutable_combined_query()->mutable_distance_range()->set_min_val(opt.combined_dist_min);
        out->mutable_combined_query()->mutable_distance_range()->set_max_val(opt.combined_dist_max);
        out->mutable_combined_query()->mutable_passenger_range()->set_min_val(opt.combined_pass_min);
        out->mutable_combined_query()->mutable_passenger_range()->set_max_val(opt.combined_pass_max);
    } else {
        throw std::runtime_error("Unsupported --query-type: " + opt.query_type);
    }
}

ClientOptions parse_cli(int argc, char** argv) {
    ClientOptions opt;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--help" || a == "-h") {
            print_usage(argv[0]);
            std::exit(0);
        }
        if (a == "--config" && i + 1 < argc) {
            opt.config_path = argv[++i];
        } else if (a == "--query-type" && i + 1 < argc) {
            opt.query_type = argv[++i];
        } else if (a == "--request-id" && i + 1 < argc) {
            opt.request_id = argv[++i];
        } else if (a == "--start-time" && i + 1 < argc) {
            const auto v = std::stoll(argv[++i]);
            opt.start_time = v;
            opt.combined_start = v;
        } else if (a == "--end-time" && i + 1 < argc) {
            const auto v = std::stoll(argv[++i]);
            opt.end_time = v;
            opt.combined_end = v;
        } else if (a == "--min" && i + 1 < argc) {
            opt.num_min = std::stod(argv[++i]);
        } else if (a == "--max" && i + 1 < argc) {
            opt.num_max = std::stod(argv[++i]);
        } else if (a == "--dist-min" && i + 1 < argc) {
            opt.combined_dist_min = std::stod(argv[++i]);
        } else if (a == "--dist-max" && i + 1 < argc) {
            opt.combined_dist_max = std::stod(argv[++i]);
        } else if (a == "--pass-min" && i + 1 < argc) {
            opt.combined_pass_min = static_cast<std::int32_t>(std::stol(argv[++i]));
        } else if (a == "--pass-max" && i + 1 < argc) {
            opt.combined_pass_max = static_cast<std::int32_t>(std::stol(argv[++i]));
        } else {
            throw std::runtime_error("Unknown or incomplete argument: " + a);
        }
    }
    if (opt.query_type.empty()) {
        throw std::runtime_error("Missing required: --query-type");
    }
    if (opt.query_type == "location") {
        opt.int_min = static_cast<std::int32_t>(static_cast<long>(opt.num_min));
        opt.int_max = static_cast<std::int32_t>(static_cast<long>(opt.num_max));
    }
    return opt;
}

} // namespace

int main(int argc, char** argv) {
    std::cout.setf(std::ios::unitbuf);
    if (argc < 2) {
        print_usage(argv[0]);
        return 1;
    }
    try {
        const auto opt = parse_cli(argc, argv);
        const auto endpoint = resolve_entry_endpoint(opt.config_path);

        mini2::QueryRequest query;
        fill_query_request(opt, &query);

        grpc::ChannelArguments channel_args;
        channel_args.SetMaxSendMessageSize(kGrpcMaxMessageBytes);
        channel_args.SetMaxReceiveMessageSize(kGrpcMaxMessageBytes);
        auto channel = grpc::CreateCustomChannel(
            endpoint,
            grpc::InsecureChannelCredentials(),
            channel_args);
        auto stub = mini2::NodeService::NewStub(channel);

        const auto t0 = std::chrono::steady_clock::now();

        mini2::ChunkResponse first;
        grpc::ClientContext submit_ctx;
        submit_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1800));
        const grpc::Status submit_status = stub->SubmitQuery(&submit_ctx, query, &first);
        const double time_to_first_chunk_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - t0).count();
        if (!submit_status.ok()) {
            std::cerr << "SubmitQuery failed: " << submit_status.error_message() << '\n';
            return 1;
        }

        const std::string& rid = first.request_id();
        std::int64_t total_records = static_cast<std::int64_t>(first.records_size());
        std::int32_t total_chunks = first.total_chunks();

        double min_chunk_rtt_ms = std::numeric_limits<double>::max();
        double max_chunk_rtt_ms = 0.0;
        double sum_chunk_rtt_ms = 0.0;
        int chunk_rtt_count = 0;

        if (!first.is_last()) {
            std::int32_t idx = 1;
            while (true) {
                mini2::ChunkRequest chunk_req;
                chunk_req.set_request_id(rid);
                chunk_req.set_chunk_index(idx);

                mini2::ChunkResponse chunk;
                grpc::ClientContext fetch_ctx;
                fetch_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1800));
                const auto chunk_t0 = std::chrono::steady_clock::now();
                const grpc::Status fetch_status = stub->FetchChunk(&fetch_ctx, chunk_req, &chunk);
                const double chunk_rtt = std::chrono::duration<double, std::milli>(
                    std::chrono::steady_clock::now() - chunk_t0).count();

                if (!fetch_status.ok()) {
                    if (fetch_status.error_code() == grpc::StatusCode::UNAVAILABLE) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(20));
                        continue;
                    }
                    std::cerr << "FetchChunk failed at index " << idx << ": "
                              << fetch_status.error_message() << '\n';
                    return 1;
                }

                std::cout << "chunk_rtt_ms[" << idx << "]: " << chunk_rtt << '\n';
                min_chunk_rtt_ms = std::min(min_chunk_rtt_ms, chunk_rtt);
                max_chunk_rtt_ms = std::max(max_chunk_rtt_ms, chunk_rtt);
                sum_chunk_rtt_ms += chunk_rtt;
                ++chunk_rtt_count;

                total_records += static_cast<std::int64_t>(chunk.records_size());
                if (chunk.total_chunks() > 0) {
                    total_chunks = chunk.total_chunks();
                }
                if (chunk.is_last()) {
                    break;
                }
                ++idx;
            }
            if (total_chunks <= 0) {
                total_chunks = idx + 1;
            }
        }

        const auto t1 = std::chrono::steady_clock::now();
        const double elapsed_ms =
            std::chrono::duration<double, std::milli>(t1 - t0).count();

        std::cout << "request_id: " << rid << '\n';
        std::cout << "entry: " << endpoint << '\n';
        std::cout << "query_type: " << opt.query_type << '\n';
        std::cout << "total_chunks: " << total_chunks << '\n';
        std::cout << "effective_chunk_size: " << first.effective_chunk_size() << '\n';
        std::cout << "total_records_received: " << total_records << '\n';
        std::cout << "time_to_first_chunk_ms: " << time_to_first_chunk_ms << '\n';
        std::cout << "elapsed_ms: " << elapsed_ms << '\n';
        if (chunk_rtt_count > 0) {
            const double avg_rtt = sum_chunk_rtt_ms / static_cast<double>(chunk_rtt_count);
            std::cout << "chunk_rtt_min_ms: " << min_chunk_rtt_ms << '\n';
            std::cout << "chunk_rtt_max_ms: " << max_chunk_rtt_ms << '\n';
            std::cout << "chunk_rtt_avg_ms: " << avg_rtt << '\n';
        }
        std::cout << "aggregation_sum: " << first.aggregation_sum() << '\n';
        std::cout << "aggregation_count: " << first.aggregation_count() << '\n';
        std::cout << "aggregation_avg: " << first.aggregation_avg() << '\n';

        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Client error: " << ex.what() << '\n';
        print_usage(argv[0]);
        return 1;
    }
}
