#include "taxi/CsvReader.hpp"
#include "taxi/QueryEngine.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <nlohmann/json.hpp>

#include "mini2.grpc.pb.h"
#include "mini2.pb.h"

namespace {

struct CliOptions {
    std::string node_id;
    std::filesystem::path config_path;
};

struct NodeRuntime {
    std::string node_id;
    std::string host;
    int port = 0;
    int chunk_size = 500;
    bool stream_up = false;
    bool bft_lite = false;
    int bft_fault_threshold = 1;
    std::string bft_auth_type = "off";
    std::unordered_map<std::string, std::vector<std::string>> bft_replica_groups;
    std::unordered_map<std::string, std::string> bft_keys;
    std::filesystem::path data_path;
    std::vector<std::string> children;
    std::unordered_map<std::string, std::string> endpoints;
    std::vector<taxi::TripRecord> records;
};

struct LocalQueryResult {
    std::vector<const taxi::TripRecord*> records;
    double aggregation_sum = 0.0;
    double aggregation_avg = 0.0;
    std::int64_t aggregation_count = 0;
};

struct ChildCallResult {
    std::string logical_child_id;
    std::string replica_id;
    ::grpc::Status status;
    mini2::ForwardResponse response;
};

struct ChildMergeResult {
    std::vector<mini2::TripRecordMsg> records;
    double aggregation_sum = 0.0;
    std::int64_t aggregation_count = 0;
};

CliOptions parse_cli(int argc, char** argv) {
    CliOptions options;
    options.config_path = "config/topology.json";

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--id" && i + 1 < argc) {
            options.node_id = argv[++i];
        } else if (arg == "--config" && i + 1 < argc) {
            options.config_path = argv[++i];
        } else {
            throw std::runtime_error("Unknown or incomplete argument: " + arg);
        }
    }

    if (options.node_id.empty()) {
        throw std::runtime_error("Missing required argument: --id");
    }
    return options;
}

std::filesystem::path resolve_data_path(
    const std::string& raw_data_path,
    const std::filesystem::path& config_path)
{
    const auto absolute_candidate = std::filesystem::path(raw_data_path);
    if (absolute_candidate.is_absolute()) {
        return absolute_candidate;
    }

    const auto config_relative =
        std::filesystem::weakly_canonical(config_path.parent_path() / raw_data_path);
    if (std::filesystem::exists(config_relative)) {
        return config_relative;
    }
    return std::filesystem::weakly_canonical(std::filesystem::current_path() / raw_data_path);
}

std::vector<std::string> derive_children(
    const nlohmann::json& config,
    const std::string& node_id,
    const std::string& root_id = "A")
{
    std::unordered_map<std::string, std::vector<std::string>> graph;
    for (const auto& edge_json : config.at("edges")) {
        const std::string edge = edge_json.get<std::string>();
        if (edge.size() < 2) {
            continue;
        }
        const std::string left(1, edge[0]);
        const std::string right(1, edge[1]);
        graph[left].push_back(right);
        graph[right].push_back(left);
    }

    std::unordered_map<std::string, std::string> parent;
    std::queue<std::string> q;
    q.push(root_id);
    parent[root_id] = "";

    while (!q.empty()) {
        const auto current = q.front();
        q.pop();
        for (const auto& neighbor : graph[current]) {
            if (parent.find(neighbor) != parent.end()) {
                continue;
            }
            parent[neighbor] = current;
            q.push(neighbor);
        }
    }

    std::vector<std::string> children;
    for (const auto& [candidate, candidate_parent] : parent) {
        if (candidate_parent == node_id) {
            children.push_back(candidate);
        }
    }
    std::sort(children.begin(), children.end());
    return children;
}

std::vector<taxi::TripRecord> load_records(const std::filesystem::path& data_path) {
    taxi::CsvReader reader(data_path.string());
    std::vector<taxi::TripRecord> records;
    records.reserve(10'000);

    taxi::TripRecord record;
    while (reader.read_next(record)) {
        records.push_back(record);
    }
    return records;
}

NodeRuntime load_runtime(const CliOptions& cli) {
    std::ifstream config_stream(cli.config_path);
    if (!config_stream.is_open()) {
        throw std::runtime_error("Failed to open config: " + cli.config_path.string());
    }

    nlohmann::json config;
    config_stream >> config;

    const auto& nodes = config.at("nodes");
    if (!nodes.contains(cli.node_id)) {
        throw std::runtime_error("Node id not found in config: " + cli.node_id);
    }

    const auto& node_cfg = nodes.at(cli.node_id);
    NodeRuntime runtime;
    runtime.node_id = cli.node_id;
    runtime.host = node_cfg.at("host").get<std::string>();
    runtime.port = node_cfg.at("port").get<int>();
    runtime.data_path = resolve_data_path(node_cfg.at("data").get<std::string>(), cli.config_path);
    if (!std::filesystem::exists(runtime.data_path)) {
        throw std::runtime_error("Data file not found: " + runtime.data_path.string());
    }
    runtime.children = derive_children(config, cli.node_id, "A");
    runtime.records = load_records(runtime.data_path);
    if (config.contains("chunk_size") && config.at("chunk_size").is_number_integer()) {
        runtime.chunk_size = config.at("chunk_size").get<int>();
        if (runtime.chunk_size < 1) {
            runtime.chunk_size = 500;
        }
    }
    runtime.stream_up = config.value("stream_up", false);
    const std::string bft_mode = config.value("bft_mode", std::string("off"));
    runtime.bft_lite = bft_mode == "lite";
    if (config.contains("bft") && config.at("bft").is_object()) {
        const auto& bft_cfg = config.at("bft");
        if (bft_cfg.contains("fault_threshold") && bft_cfg.at("fault_threshold").is_number_integer()) {
            runtime.bft_fault_threshold = std::max(0, bft_cfg.at("fault_threshold").get<int>());
        }
        if (bft_cfg.contains("replica_groups") && bft_cfg.at("replica_groups").is_object()) {
            for (auto it = bft_cfg.at("replica_groups").begin(); it != bft_cfg.at("replica_groups").end(); ++it) {
                if (!it.value().is_array()) {
                    continue;
                }
                std::vector<std::string> replicas;
                for (const auto& node_json : it.value()) {
                    if (node_json.is_string()) {
                        replicas.push_back(node_json.get<std::string>());
                    }
                }
                if (!replicas.empty()) {
                    runtime.bft_replica_groups[it.key()] = std::move(replicas);
                }
            }
        }
        if (bft_cfg.contains("auth") && bft_cfg.at("auth").is_object()) {
            const auto& auth_cfg = bft_cfg.at("auth");
            runtime.bft_auth_type = auth_cfg.value("type", std::string("off"));
            if (auth_cfg.contains("keys") && auth_cfg.at("keys").is_object()) {
                for (auto it = auth_cfg.at("keys").begin(); it != auth_cfg.at("keys").end(); ++it) {
                    if (it.value().is_string()) {
                        runtime.bft_keys[it.key()] = it.value().get<std::string>();
                    }
                }
            }
        }
    }
    for (auto it = nodes.begin(); it != nodes.end(); ++it) {
        runtime.endpoints[it.key()] =
            it.value().at("host").get<std::string>() + ":" +
            std::to_string(it.value().at("port").get<int>());
    }
    return runtime;
}

void run_local_query_path(const std::vector<taxi::TripRecord>& records) {
    taxi::QueryEngine engine(records);
    const double build_ms = engine.build_indexes();

    std::int64_t min_pickup = std::numeric_limits<std::int64_t>::max();
    std::int64_t max_pickup = std::numeric_limits<std::int64_t>::min();
    for (const auto& rec : records) {
        min_pickup = std::min(min_pickup, rec.pickup_timestamp);
        max_pickup = std::max(max_pickup, rec.pickup_timestamp);
    }
    if (records.empty()) {
        min_pickup = 0;
        max_pickup = 0;
    }

    const taxi::TimeRangeQuery warmup_query {min_pickup, max_pickup};
    const auto warmup_result = engine.search_by_time(warmup_query);

    std::cout << "  index_build_ms: " << build_ms << '\n';
    std::cout << "  local_query_warmup_count: " << warmup_result.records.size() << '\n';
}

std::string make_request_id() {
    static std::atomic<std::uint64_t> seq {0};
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return "cpp-" + std::to_string(now_ms) + "-" + std::to_string(seq.fetch_add(1));
}

std::string fnv1a64_hex(const std::string& payload)
{
    std::uint64_t hash = 1469598103934665603ull;
    for (unsigned char c : payload) {
        hash ^= static_cast<std::uint64_t>(c);
        hash *= 1099511628211ull;
    }
    std::ostringstream out;
    out << std::hex << std::setw(16) << std::setfill('0') << hash;
    return out.str();
}

std::string build_forward_payload_hash(const mini2::ForwardResponse& response)
{
    std::ostringstream payload;
    payload << response.request_id() << '|'
            << response.source_node() << '|'
            << response.aggregation_sum() << '|'
            << response.aggregation_avg() << '|'
            << response.aggregation_count() << '|'
            << response.records_size();
    for (const auto& rec : response.records()) {
        payload << '|'
                << rec.vendor_id() << ','
                << rec.pickup_timestamp() << ','
                << rec.dropoff_timestamp() << ','
                << rec.passenger_count() << ','
                << rec.trip_distance() << ','
                << rec.rate_code_id() << ','
                << rec.store_and_fwd_flag() << ','
                << rec.pu_location_id() << ','
                << rec.do_location_id() << ','
                << rec.payment_type() << ','
                << rec.fare_amount() << ','
                << rec.extra() << ','
                << rec.mta_tax() << ','
                << rec.tip_amount() << ','
                << rec.tolls_amount() << ','
                << rec.improvement_surcharge() << ','
                << rec.total_amount();
    }
    return fnv1a64_hex(payload.str());
}

std::string build_auth_tag(
    const std::string& node_id,
    const std::string& request_id,
    const std::string& payload_hash,
    const std::string& shared_key)
{
    return fnv1a64_hex(node_id + "|" + request_id + "|" + payload_hash + "|" + shared_key);
}

void append_record_to_proto(const taxi::TripRecord& rec, mini2::TripRecordMsg* out) {
    out->set_vendor_id(static_cast<int>(rec.vendor_id));
    out->set_pickup_timestamp(rec.pickup_timestamp);
    out->set_dropoff_timestamp(rec.dropoff_timestamp);
    out->set_passenger_count(static_cast<int>(rec.passenger_count));
    out->set_trip_distance(rec.trip_distance);
    out->set_rate_code_id(static_cast<int>(rec.rate_code_id));
    out->set_store_and_fwd_flag(rec.store_and_fwd_flag != 0);
    out->set_pu_location_id(static_cast<int>(rec.pu_location_id));
    out->set_do_location_id(static_cast<int>(rec.do_location_id));
    out->set_payment_type(static_cast<int>(rec.payment_type));
    out->set_fare_amount(rec.fare_amount);
    out->set_extra(rec.extra);
    out->set_mta_tax(rec.mta_tax);
    out->set_tip_amount(rec.tip_amount);
    out->set_tolls_amount(rec.tolls_amount);
    out->set_improvement_surcharge(rec.improvement_surcharge);
    out->set_total_amount(rec.total_amount);
}

LocalQueryResult execute_local_query(taxi::QueryEngine& engine, const mini2::QueryRequest& request) {
    LocalQueryResult result;

    if (request.query_type() == "time") {
        taxi::TimeRangeQuery q {request.time_query().start_time(), request.time_query().end_time()};
        auto qr = engine.search_by_time(q);
        result.records = std::move(qr.records);
    } else if (request.query_type() == "distance") {
        taxi::NumericRangeQuery q {request.numeric_query().min_val(), request.numeric_query().max_val()};
        auto qr = engine.search_by_distance(q);
        result.records = std::move(qr.records);
    } else if (request.query_type() == "fare") {
        taxi::NumericRangeQuery q {request.numeric_query().min_val(), request.numeric_query().max_val()};
        auto qr = engine.search_by_fare(q);
        result.records = std::move(qr.records);
    } else if (request.query_type() == "location") {
        taxi::IntRangeQuery q {request.int_query().min_val(), request.int_query().max_val()};
        auto qr = engine.search_by_location(q);
        result.records = std::move(qr.records);
    } else if (request.query_type() == "combined") {
        taxi::CombinedQuery q {
            taxi::TimeRangeQuery {
                request.combined_query().time_range().start_time(),
                request.combined_query().time_range().end_time(),
            },
            taxi::NumericRangeQuery {
                request.combined_query().distance_range().min_val(),
                request.combined_query().distance_range().max_val(),
            },
            taxi::IntRangeQuery {
                request.combined_query().passenger_range().min_val(),
                request.combined_query().passenger_range().max_val(),
            },
        };
        auto qr = engine.search_combined(q);
        result.records = std::move(qr.records);
    } else if (request.query_type() == "aggregate") {
        taxi::TimeRangeQuery q {request.time_query().start_time(), request.time_query().end_time()};
        const auto agg = engine.aggregate_fare_by_time(q);
        result.aggregation_sum = agg.sum;
        result.aggregation_count = static_cast<std::int64_t>(agg.count);
        result.aggregation_avg = agg.avg;
        return result;
    } else {
        throw std::runtime_error("Unsupported query_type: " + request.query_type());
    }

    for (const auto* rec : result.records) {
        result.aggregation_sum += rec->fare_amount;
    }
    result.aggregation_count = static_cast<std::int64_t>(result.records.size());
    result.aggregation_avg = result.aggregation_count == 0
        ? 0.0
        : result.aggregation_sum / static_cast<double>(result.aggregation_count);
    return result;
}

struct ChunkCacheEntry {
    std::int32_t total_chunks = 0;
    std::int32_t records_per_chunk = 0;
    double aggregation_sum = 0.0;
    double aggregation_avg = 0.0;
    std::int64_t aggregation_count = 0;
    bool finalized = false;
    // Chunk indices 1 .. total_chunks-1 (chunk 0 is returned only from SubmitQuery).
    std::vector<std::vector<mini2::TripRecordMsg>> chunks_after_first;
    std::vector<mini2::TripRecordMsg> stream_records_after_first;
};

class NodeServiceImpl final : public mini2::NodeService::Service {
public:
    explicit NodeServiceImpl(const NodeRuntime& runtime)
        : runtime_(runtime), engine_(runtime_.records)
    {
        dynamic_chunk_size_.store(
            std::clamp(runtime_.chunk_size, 50, 2000), std::memory_order_relaxed);
        const double build_ms = engine_.build_indexes();
        std::cout << "Node " << runtime_.node_id << " query index initialized in "
                  << build_ms << " ms\n";
        for (const auto& child : runtime_.children) {
            auto endpoint_it = runtime_.endpoints.find(child);
            if (endpoint_it == runtime_.endpoints.end()) {
                continue;
            }
            child_stubs_[child] = mini2::NodeService::NewStub(
                grpc::CreateChannel(endpoint_it->second, grpc::InsecureChannelCredentials()));
        }
    }

    ::grpc::Status SubmitQuery(
        ::grpc::ServerContext*,
        const mini2::QueryRequest* request,
        mini2::ChunkResponse* response) override
    {
        const auto serve_t0 = std::chrono::steady_clock::now();
        const std::string request_id =
            request->request_id().empty() ? make_request_id() : request->request_id();

        mini2::QueryRequest effective_request = *request;
        effective_request.set_request_id(request_id);

        auto cancel_flag = std::make_shared<std::atomic<bool>>(false);
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            canceled_ids_.erase(request_id);
            active_cancel_[request_id] = cancel_flag;
        }

        LocalQueryResult local_result;
        try {
            local_result = execute_local_query(engine_, effective_request);
        } catch (const std::exception& ex) {
            std::lock_guard<std::mutex> lock(state_mutex_);
            active_cancel_.erase(request_id);
            return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, ex.what());
        }

        double merged_sum = local_result.aggregation_sum;
        std::int64_t merged_count = local_result.aggregation_count;

        std::vector<mini2::TripRecordMsg> merged_records;
        merged_records.reserve(
            local_result.records.size() + static_cast<std::size_t>(runtime_.children.size()) * 500);

        for (const auto* rec : local_result.records) {
            mini2::TripRecordMsg msg;
            append_record_to_proto(*rec, &msg);
            merged_records.push_back(std::move(msg));
        }

        const auto replica_targets = expand_replica_targets();
        std::vector<std::future<ChildCallResult>> futures;
        futures.reserve(replica_targets.size());
        for (const auto& target : replica_targets) {
            auto stub_it = child_stubs_.find(target.replica_id);
            if (stub_it == child_stubs_.end()) {
                continue;
            }
            mini2::ForwardRequest forward_request;
            forward_request.set_request_id(request_id);
            forward_request.set_origin_node(runtime_.node_id);
            *forward_request.mutable_query() = effective_request;
            forward_request.mutable_bft_meta()->set_node_id(runtime_.node_id);

            mini2::NodeService::Stub* stub = stub_it->second.get();
            futures.push_back(std::async(
                std::launch::async,
                [target, forward_request, stub]() mutable {
                ChildCallResult result;
                result.logical_child_id = target.logical_child_id;
                result.replica_id = target.replica_id;
                ::grpc::ClientContext client_ctx;
                client_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
                result.status = stub->ForwardQuery(&client_ctx, forward_request, &result.response);
                return result;
            }));
        }

        if (runtime_.stream_up) {
            const int chunk_sz = dynamic_chunk_size_.load(std::memory_order_relaxed);
            const int local_total_records = static_cast<int>(merged_records.size());
            const int first_end = std::min(chunk_sz, local_total_records);

            response->set_request_id(request_id);
            response->set_chunk_index(0);
            response->set_total_chunks(0);
            response->set_is_last(futures.empty() && local_total_records <= chunk_sz);
            response->set_aggregation_sum(local_result.aggregation_sum);
            response->set_aggregation_count(local_result.aggregation_count);
            response->set_aggregation_avg(local_result.aggregation_avg);
            response->set_effective_chunk_size(chunk_sz);
            for (int i = 0; i < first_end; ++i) {
                *response->add_records() = merged_records[static_cast<std::size_t>(i)];
            }

            if (!response->is_last()) {
                ChunkCacheEntry entry;
                entry.records_per_chunk = chunk_sz;
                entry.aggregation_sum = local_result.aggregation_sum;
                entry.aggregation_avg = local_result.aggregation_avg;
                entry.aggregation_count = local_result.aggregation_count;
                entry.finalized = futures.empty();
                for (int i = first_end; i < local_total_records; ++i) {
                    entry.stream_records_after_first.push_back(
                        merged_records[static_cast<std::size_t>(i)]);
                }
                if (entry.finalized) {
                    rebuild_chunk_views(entry);
                    entry.total_chunks =
                        1 + static_cast<std::int32_t>(entry.chunks_after_first.size());
                }
                std::lock_guard<std::mutex> lock(state_mutex_);
                if (!cancel_flag->load(std::memory_order_acquire)) {
                    chunk_cache_[request_id] = std::move(entry);
                }
            }

            double first_chunk_fare_sum = 0.0;
            for (int i = 0; i < first_end; ++i) {
                first_chunk_fare_sum += merged_records[static_cast<std::size_t>(i)].fare_amount();
            }

            if (!futures.empty()) {
                std::thread([this, request_id, cancel_flag, query_type = effective_request.query_type(),
                             first_chunk_count = static_cast<std::int64_t>(first_end),
                             first_chunk_fare_sum,
                             futures = std::move(futures)]() mutable {
                    std::vector<ChildCallResult> child_results;
                    child_results.reserve(futures.size());
                    for (auto& future : futures) {
                        if (cancel_flag->load(std::memory_order_acquire)) {
                            return;
                        }
                        child_results.push_back(future.get());
                    }
                    const ChildMergeResult merged_child = collect_child_merge_result(request_id, child_results);

                    std::lock_guard<std::mutex> lock(state_mutex_);
                    if (canceled_ids_.count(request_id) != 0) {
                        return;
                    }
                    auto cache_it = chunk_cache_.find(request_id);
                    if (cache_it == chunk_cache_.end()) {
                        return;
                    }
                    auto& entry = cache_it->second;
                    entry.aggregation_sum += merged_child.aggregation_sum;
                    entry.aggregation_count += merged_child.aggregation_count;
                    for (const auto& record : merged_child.records) {
                        entry.stream_records_after_first.push_back(record);
                    }
                    if (query_type != "aggregate") {
                        entry.aggregation_count =
                            first_chunk_count + static_cast<std::int64_t>(entry.stream_records_after_first.size());
                        double sum = first_chunk_fare_sum;
                        for (const auto& rec : entry.stream_records_after_first) {
                            sum += rec.fare_amount();
                        }
                        entry.aggregation_sum = sum;
                    }
                    entry.aggregation_avg = entry.aggregation_count == 0
                        ? 0.0
                        : entry.aggregation_sum / static_cast<double>(entry.aggregation_count);
                    rebuild_chunk_views(entry);
                    entry.finalized = true;
                    entry.total_chunks =
                        1 + static_cast<std::int32_t>(entry.chunks_after_first.size());
                    std::cout << "[A] request_id=" << request_id
                              << " stream-up finalized total_chunks=" << entry.total_chunks
                              << '\n';
                    active_cancel_.erase(request_id);
                }).detach();
            } else {
                std::lock_guard<std::mutex> lock(state_mutex_);
                active_cancel_.erase(request_id);
                canceled_ids_.erase(request_id);
            }

            const double submit_serve_ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - serve_t0).count();
            tune_after_chunk_served(submit_serve_ms);
            return ::grpc::Status::OK;
        }

        std::cout << "[A] request_id=" << request_id << " fan-out children=" << replica_targets.size() << '\n';
        std::vector<ChildCallResult> child_results;
        child_results.reserve(futures.size());
        for (auto& future : futures) {
            if (cancel_flag->load(std::memory_order_acquire)) {
                break;
            }
            child_results.push_back(future.get());
        }
        const ChildMergeResult merged_child = collect_child_merge_result(request_id, child_results);
        if (!cancel_flag->load(std::memory_order_acquire)) {
            merged_sum += merged_child.aggregation_sum;
            merged_count += merged_child.aggregation_count;
            for (const auto& record : merged_child.records) {
                merged_records.push_back(record);
            }
        }
        if (cancel_flag->load(std::memory_order_acquire)) {
            for (auto& future : futures) {
                (void)future.wait_for(std::chrono::milliseconds(0));
            }
        }

        if (effective_request.query_type() != "aggregate") {
            merged_count = static_cast<std::int64_t>(merged_records.size());
            merged_sum = 0.0;
            for (const auto& msg : merged_records) {
                merged_sum += msg.fare_amount();
            }
        }

        const double merged_avg =
            merged_count == 0 ? 0.0 : merged_sum / static_cast<double>(merged_count);

        if (cancel_flag->load(std::memory_order_acquire)) {
            std::lock_guard<std::mutex> lock(state_mutex_);
            active_cancel_.erase(request_id);
            chunk_cache_.erase(request_id);
            canceled_ids_.insert(request_id);
            return ::grpc::Status(::grpc::StatusCode::CANCELLED, "query canceled");
        }

        const int chunk_sz = dynamic_chunk_size_.load(std::memory_order_relaxed);
        const int total_records = static_cast<int>(merged_records.size());
        std::int32_t total_chunks = 1;
        if (total_records > 0) {
            total_chunks = static_cast<std::int32_t>(
                (total_records + chunk_sz - 1) / chunk_sz);
        }

        response->set_request_id(request_id);
        response->set_chunk_index(0);
        response->set_total_chunks(total_chunks);
        response->set_is_last(total_chunks <= 1);
        response->set_aggregation_sum(merged_sum);
        response->set_aggregation_count(merged_count);
        response->set_aggregation_avg(merged_avg);
        response->set_effective_chunk_size(chunk_sz);

        const int first_end = std::min(chunk_sz, total_records);
        for (int i = 0; i < first_end; ++i) {
            *response->add_records() = merged_records[static_cast<std::size_t>(i)];
        }

        if (total_chunks > 1) {
            ChunkCacheEntry entry;
            entry.total_chunks = total_chunks;
            entry.records_per_chunk = chunk_sz;
            entry.aggregation_sum = merged_sum;
            entry.aggregation_avg = merged_avg;
            entry.aggregation_count = merged_count;
            entry.finalized = true;
            entry.chunks_after_first.reserve(static_cast<std::size_t>(total_chunks - 1));
            for (std::int32_t c = 1; c < total_chunks; ++c) {
                const int start = static_cast<int>(c) * chunk_sz;
                const int end = std::min(start + chunk_sz, total_records);
                std::vector<mini2::TripRecordMsg> slice;
                slice.reserve(static_cast<std::size_t>(end - start));
                for (int i = start; i < end; ++i) {
                    slice.push_back(merged_records[static_cast<std::size_t>(i)]);
                }
                entry.chunks_after_first.push_back(std::move(slice));
            }
            std::lock_guard<std::mutex> lock(state_mutex_);
            if (!cancel_flag->load(std::memory_order_acquire)) {
                chunk_cache_[request_id] = std::move(entry);
            }
        }

        std::cout << "[A] request_id=" << request_id
                  << " merged_records=" << total_records
                  << " total_chunks=" << total_chunks
                  << " local_rows=" << local_result.records.size() << '\n';

        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            active_cancel_.erase(request_id);
            canceled_ids_.erase(request_id);
        }

        const double submit_serve_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - serve_t0).count();
        tune_after_chunk_served(submit_serve_ms);

        return ::grpc::Status::OK;
    }

    ::grpc::Status FetchChunk(
        ::grpc::ServerContext*,
        const mini2::ChunkRequest* request,
        mini2::ChunkResponse* response) override
    {
        const auto serve_t0 = std::chrono::steady_clock::now();
        const std::string& rid = request->request_id();
        const std::int32_t idx = request->chunk_index();

        std::unique_lock<std::mutex> lock(state_mutex_);
        if (canceled_ids_.count(rid) != 0) {
            return ::grpc::Status(::grpc::StatusCode::NOT_FOUND, "request canceled");
        }
        auto it = chunk_cache_.find(rid);
        if (it == chunk_cache_.end()) {
            return ::grpc::Status(::grpc::StatusCode::NOT_FOUND, "unknown request_id or chunk");
        }
        const auto& entry = it->second;
        if (idx <= 0) {
            return ::grpc::Status(::grpc::StatusCode::NOT_FOUND, "invalid chunk_index");
        }

        response->set_request_id(rid);
        response->set_chunk_index(idx);
        response->set_total_chunks(entry.finalized ? entry.total_chunks : 0);
        response->set_aggregation_sum(entry.aggregation_sum);
        response->set_aggregation_avg(entry.aggregation_avg);
        response->set_aggregation_count(entry.aggregation_count);
        response->set_effective_chunk_size(entry.records_per_chunk);

        const std::size_t slot = static_cast<std::size_t>(idx - 1);
        if (slot >= entry.chunks_after_first.size()) {
            if (!entry.finalized) {
                return ::grpc::Status(::grpc::StatusCode::UNAVAILABLE, "chunk not ready yet, retry");
            }
            return ::grpc::Status(::grpc::StatusCode::NOT_FOUND, "invalid chunk_index");
        }
        for (const auto& rec : entry.chunks_after_first[slot]) {
            *response->add_records() = rec;
        }
        const bool is_last = entry.finalized && idx == entry.total_chunks - 1;
        response->set_is_last(is_last);
        if (is_last) {
            chunk_cache_.erase(rid);
            std::cout << "[A] request_id=" << rid << " cache evicted after last chunk\n";
        }
        lock.unlock();

        const double fetch_serve_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - serve_t0).count();
        tune_after_chunk_served(fetch_serve_ms);

        return ::grpc::Status::OK;
    }

    ::grpc::Status CancelQuery(
        ::grpc::ServerContext*,
        const mini2::CancelRequest* request,
        mini2::CancelResponse* response) override
    {
        const std::string& rid = request->request_id();
        std::lock_guard<std::mutex> lock(state_mutex_);
        canceled_ids_.insert(rid);
        if (auto it = active_cancel_.find(rid); it != active_cancel_.end()) {
            it->second->store(true, std::memory_order_release);
        }
        chunk_cache_.erase(rid);
        response->set_acknowledged(true);
        std::cout << "[A] request_id=" << rid << " cancel acknowledged\n";
        return ::grpc::Status::OK;
    }

private:
    struct ReplicaTarget {
        std::string logical_child_id;
        std::string replica_id;
    };

    std::vector<ReplicaTarget> expand_replica_targets() const
    {
        std::vector<ReplicaTarget> out;
        for (const auto& logical_child : runtime_.children) {
            auto it = runtime_.bft_replica_groups.find(logical_child);
            if (runtime_.bft_lite && it != runtime_.bft_replica_groups.end() && !it->second.empty()) {
                for (const auto& replica_id : it->second) {
                    if (child_stubs_.find(replica_id) != child_stubs_.end()) {
                        out.push_back(ReplicaTarget {logical_child, replica_id});
                    }
                }
            } else {
                if (child_stubs_.find(logical_child) != child_stubs_.end()) {
                    out.push_back(ReplicaTarget {logical_child, logical_child});
                }
            }
        }
        return out;
    }

    bool verify_bft_metadata(
        const std::string& request_id,
        const ChildCallResult& result) const
    {
        if (!runtime_.bft_lite) {
            return true;
        }
        if (!result.response.has_bft_meta()) {
            std::cout << "[A][BFT] request_id=" << request_id
                      << " child=" << result.replica_id
                      << " rejected: missing bft_meta\n";
            return false;
        }
        const auto& meta = result.response.bft_meta();
        if (meta.node_id() != result.replica_id) {
            std::cout << "[A][BFT] request_id=" << request_id
                      << " child=" << result.replica_id
                      << " rejected: meta.node_id mismatch (" << meta.node_id() << ")\n";
            return false;
        }
        if (meta.payload_hash().empty()) {
            std::cout << "[A][BFT] request_id=" << request_id
                      << " child=" << result.replica_id
                      << " rejected: missing payload_hash\n";
            return false;
        }
        if (meta.auth_tag().empty()) {
            std::cout << "[A][BFT] request_id=" << request_id
                      << " child=" << result.replica_id
                      << " rejected: missing auth_tag\n";
            return false;
        }
        return true;
    }

    ChildMergeResult collect_child_merge_result(
        const std::string& request_id,
        const std::vector<ChildCallResult>& child_results) const
    {
        ChildMergeResult merged;
        std::unordered_map<std::string, std::vector<const ChildCallResult*>> by_logical;
        for (const auto& result : child_results) {
            by_logical[result.logical_child_id].push_back(&result);
        }

        for (const auto& logical_child : runtime_.children) {
            const auto it = by_logical.find(logical_child);
            if (it == by_logical.end()) {
                continue;
            }

            std::vector<const ChildCallResult*> valid;
            valid.reserve(it->second.size());
            std::unordered_map<std::string, std::vector<const ChildCallResult*>> by_hash;
            for (const ChildCallResult* result : it->second) {
                if (!result->status.ok()) {
                    std::cout << "[A] request_id=" << request_id
                              << " child=" << result->replica_id
                              << " forward failed: " << result->status.error_message() << '\n';
                    continue;
                }
                if (result->response.request_id() != request_id) {
                    std::cout << "[A] request_id=" << request_id
                              << " child=" << result->replica_id
                              << " returned mismatched request_id="
                              << result->response.request_id() << '\n';
                    continue;
                }
                if (!verify_bft_metadata(request_id, *result)) {
                    continue;
                }
                valid.push_back(result);
                const std::string payload_hash = runtime_.bft_lite && result->response.has_bft_meta()
                    ? result->response.bft_meta().payload_hash()
                    : build_forward_payload_hash(result->response);
                by_hash[payload_hash].push_back(result);
            }
            if (valid.empty()) {
                continue;
            }

            std::size_t quorum = 1;
            if (runtime_.bft_lite) {
                std::size_t configured_replicas = it->second.size();
                auto cfg_it = runtime_.bft_replica_groups.find(logical_child);
                if (cfg_it != runtime_.bft_replica_groups.end()) {
                    configured_replicas = cfg_it->second.size();
                }
                const std::size_t maj = configured_replicas / 2 + 1;
                const std::size_t resilient =
                    configured_replicas > static_cast<std::size_t>(runtime_.bft_fault_threshold)
                    ? configured_replicas - static_cast<std::size_t>(runtime_.bft_fault_threshold)
                    : configured_replicas;
                quorum = std::max<std::size_t>(1, std::max(maj, resilient));
            }

            const std::vector<const ChildCallResult*>* winner_bucket = nullptr;
            for (const auto& [hash_key, bucket] : by_hash) {
                (void)hash_key;
                if (winner_bucket == nullptr || bucket.size() > winner_bucket->size()) {
                    winner_bucket = &bucket;
                }
            }
            if (winner_bucket == nullptr || winner_bucket->size() < quorum) {
                std::cout << "[A][BFT] request_id=" << request_id
                          << " logical_child=" << logical_child
                          << " rejected: quorum not met (have="
                          << (winner_bucket == nullptr ? 0 : winner_bucket->size())
                          << ", need=" << quorum << ")\n";
                continue;
            }

            if (runtime_.bft_lite && by_hash.size() > 1) {
                for (const auto& [hash_key, bucket] : by_hash) {
                    if (&bucket == winner_bucket) {
                        continue;
                    }
                    for (const ChildCallResult* outlier : bucket) {
                        std::cout << "[A][BFT] request_id=" << request_id
                                  << " logical_child=" << logical_child
                                  << " excluded_replica=" << outlier->replica_id
                                  << " reason=payload_mismatch\n";
                    }
                }
            }

            const ChildCallResult* selected = winner_bucket->front();
            merged.aggregation_sum += selected->response.aggregation_sum();
            merged.aggregation_count += selected->response.aggregation_count();
            for (const auto& record : selected->response.records()) {
                merged.records.push_back(record);
            }
        }
        return merged;
    }

    static void rebuild_chunk_views(ChunkCacheEntry& entry)
    {
        entry.chunks_after_first.clear();
        const int chunk_sz = std::max(1, entry.records_per_chunk);
        for (std::size_t pos = 0; pos < entry.stream_records_after_first.size(); pos += chunk_sz) {
            const std::size_t end = std::min(
                pos + static_cast<std::size_t>(chunk_sz), entry.stream_records_after_first.size());
            std::vector<mini2::TripRecordMsg> chunk;
            chunk.reserve(end - pos);
            for (std::size_t i = pos; i < end; ++i) {
                chunk.push_back(entry.stream_records_after_first[i]);
            }
            entry.chunks_after_first.push_back(std::move(chunk));
        }
    }

    void tune_after_chunk_served(double serve_ms)
    {
        int cur = dynamic_chunk_size_.load(std::memory_order_relaxed);
        int next = cur;
        if (serve_ms < 10.0) {
            next = static_cast<int>(std::lround(static_cast<double>(cur) * 1.2));
        } else if (serve_ms > 100.0) {
            next = static_cast<int>(std::lround(static_cast<double>(cur) * 0.8));
        }
        next = std::clamp(next, 50, 2000);
        if (next != cur) {
            std::cout << '[' << runtime_.node_id << "] dynamic_chunk_size " << cur << " -> " << next
                      << " (serve_ms=" << serve_ms << ")\n";
            dynamic_chunk_size_.store(next, std::memory_order_relaxed);
        }
    }

    const NodeRuntime& runtime_;
    taxi::QueryEngine engine_;
    std::unordered_map<std::string, std::unique_ptr<mini2::NodeService::Stub>> child_stubs_;

    std::mutex state_mutex_;
    std::unordered_map<std::string, ChunkCacheEntry> chunk_cache_;
    std::unordered_map<std::string, std::shared_ptr<std::atomic<bool>>> active_cancel_;
    std::unordered_set<std::string> canceled_ids_;
    std::atomic<int> dynamic_chunk_size_;
};

void run_server(const NodeRuntime& runtime) {
    NodeServiceImpl service(runtime);
    grpc::ServerBuilder builder;
    builder.AddChannelArgument("grpc.so_reuseport", 0);
    const std::string listen_addr = runtime.host + ":" + std::to_string(runtime.port);
    builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server) {
        throw std::runtime_error("Failed to start gRPC server on " + listen_addr);
    }
    std::cout << "Node " << runtime.node_id << " gRPC server listening on " << listen_addr << '\n';
    server->Wait();
}

} // namespace

int main(int argc, char** argv) {
    std::cout.setf(std::ios::unitbuf);
    try {
        const auto cli = parse_cli(argc, argv);
        const auto runtime = load_runtime(cli);

        std::cout << "Node " << cli.node_id << " startup complete\n";
        std::cout << "  config: " << std::filesystem::weakly_canonical(cli.config_path).string() << '\n';
        std::cout << "  host: " << runtime.host << '\n';
        std::cout << "  port: " << runtime.port << '\n';
        std::cout << "  data_file: " << runtime.data_path.string() << '\n';
        std::cout << "  loaded_rows: " << runtime.records.size() << '\n';
        std::cout << "  chunk_size: " << runtime.chunk_size << '\n';
        std::cout << "  stream_up: " << (runtime.stream_up ? "true" : "false") << '\n';
        std::cout << "  bft_mode: " << (runtime.bft_lite ? "lite" : "off") << '\n';
        std::cout << "  children: ";
        if (runtime.children.empty()) {
            std::cout << "(none)\n";
        } else {
            for (std::size_t i = 0; i < runtime.children.size(); ++i) {
                std::cout << runtime.children[i];
                if (i + 1 < runtime.children.size()) {
                    std::cout << ", ";
                }
            }
            std::cout << '\n';
        }

        run_local_query_path(runtime.records);
        run_server(runtime);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Node startup failed: " << ex.what() << '\n';
        return 1;
    }
}
