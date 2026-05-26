# =============================================================================
# 文件：test/unit/CMakeLists.txt
# 作用：构建单元测试可执行文件 raftkv_unit_tests（GoogleTest 1.14，FetchContent）
# 链接：GTest、gtest_main、Boost::serialization、pthread、Persister.cpp
# 不链接：gRPC、protobuf RPC、完整 skip_list_on_raft（与集成测试 kv_bench 分离）
# 注册：gtest_discover_tests → CI 中 cmake-build 目录执行 ctest --output-on-failure
# 产物路径：${PROJECT_SOURCE_DIR}/bin/raftkv_unit_tests
# =============================================================================

include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

find_package(Boost REQUIRED COMPONENTS serialization)

add_executable(raftkv_unit_tests
    skip_list_test.cpp
    op_test.cpp
    persister_test.cpp
    ${CMAKE_SOURCE_DIR}/src/raftCore/Persister.cpp
)

target_include_directories(raftkv_unit_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/src/common/include
    ${CMAKE_SOURCE_DIR}/src/raftCore/include
    ${CMAKE_SOURCE_DIR}/src/skipList/include
)

target_link_libraries(raftkv_unit_tests PRIVATE
    GTest::gtest_main
    Boost::serialization
    pthread
)

# 覆盖根 CMakeLists 的 EXECUTABLE_OUTPUT_PATH，保证 ctest 在 cmake-build 下能找到可执行文件
set_target_properties(raftkv_unit_tests PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
)

include(GoogleTest)
gtest_discover_tests(raftkv_unit_tests WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
