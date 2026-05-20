add_executable(kv_bench kv_bench.cpp ${src_raftClerk})
target_link_libraries(kv_bench skip_list_on_raft protobuf::libprotobuf boost_serialization pthread)

add_executable(kv_consistency kv_consistency.cpp ${src_raftClerk})
target_link_libraries(kv_consistency skip_list_on_raft protobuf::libprotobuf boost_serialization pthread)

