# =============================================================================
# test/ 目录总览
#   kv_bench / kv_consistency — 集成测试（需集群 test.conf，链 skip_list_on_raft + gRPC）
#   unit/                   — 单元测试（见 test/unit/*.cpp 文件头注释，共 12 用例）
# =============================================================================

add_executable(kv_bench kv_bench.cpp ${src_raftClerk})target_link_libraries(kv_bench skip_list_on_raft protobuf::libprotobuf boost_serialization pthread)

add_executable(kv_consistency kv_consistency.cpp ${src_raftClerk})
target_link_libraries(kv_consistency skip_list_on_raft protobuf::libprotobuf boost_serialization pthread)

add_subdirectory(unit)
