cmake_minimum_required(VERSION 3.20)
project(cmpe275_mini2_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(gRPC REQUIRED)
find_package(nlohmann_json REQUIRED)

add_library(mini2_proto
    mini2.pb.cc
    mini2.grpc.pb.cc
)
target_include_directories(mini2_proto PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(mini2_proto PUBLIC
    protobuf::libprotobuf
    gRPC::grpc++
)

add_library(taxi_core
    src/TripRecord.cpp
    src/CsvReader.cpp
    src/QueryEngine.cpp
    src/TimeIndex.cpp
)
target_include_directories(taxi_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_executable(node_server
    server/node_server.cpp
)
target_link_libraries(node_server PRIVATE
    mini2_proto
    taxi_core
    nlohmann_json::nlohmann_json
)

add_executable(client
    client/client.cpp
)
target_link_libraries(client PRIVATE
    mini2_proto
    nlohmann_json::nlohmann_json
)
