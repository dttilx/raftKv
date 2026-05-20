set(SRC_LIST raftKvDB.cpp)


add_executable(raftCoreRun ${SRC_LIST})
target_link_libraries(raftCoreRun skip_list_on_raft protobuf::libprotobuf pthread)

#################################

set(SRC_LIST2 caller.cpp)
add_executable(callerMain ${src_raftClerk} ${SRC_LIST2})
target_link_libraries(callerMain skip_list_on_raft  protobuf::libprotobuf boost_serialization )
