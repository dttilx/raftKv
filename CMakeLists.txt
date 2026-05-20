# 设置cmake的最低版本和项目名称
cmake_minimum_required(VERSION 3.22)
project(KVRaftCpp)

find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
    find_package(Protobuf REQUIRED)
endif()
if(TARGET Protobuf::libprotobuf AND NOT TARGET protobuf::libprotobuf)
    add_library(protobuf::libprotobuf ALIAS Protobuf::libprotobuf)
endif()
if(TARGET Protobuf::protoc AND NOT TARGET protobuf::protoc)
    add_executable(protobuf::protoc ALIAS Protobuf::protoc)
endif()
find_package(gRPC CONFIG QUIET)
if(NOT gRPC_FOUND)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(GRPCPP REQUIRED IMPORTED_TARGET grpc++)
    if(NOT TARGET gRPC::grpc++)
        add_library(gRPC::grpc++ INTERFACE IMPORTED)
        target_link_libraries(gRPC::grpc++ INTERFACE PkgConfig::GRPCPP)
    endif()
    find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)
    if(NOT TARGET gRPC::grpc_cpp_plugin)
        add_executable(gRPC::grpc_cpp_plugin IMPORTED)
        set_target_properties(gRPC::grpc_cpp_plugin PROPERTIES IMPORTED_LOCATION "${GRPC_CPP_PLUGIN}")
    endif()
endif()





set(CMAKE_CXX_STANDARD 20)
# 默认生成带调试信息的版本（方便 perf/gdb），允许用户通过 -DCMAKE_BUILD_TYPE 覆盖
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING "Build type" FORCE)
endif()

# perf 火焰图/栈回溯更稳定（frame pointer），并保留符号
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer")

# 设置项目可执行文件输出的路径
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
# 设置项目库文件输出的路径
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

# 设置项目编译头文件搜索路径 -I
# 目前可能存在路径污染的问题，需要进一步解决
include_directories(${PROJECT_SOURCE_DIR}/src/common/include)
include_directories(${PROJECT_SOURCE_DIR}/example)
include_directories(${PROJECT_SOURCE_DIR}/src/raftCore/include)
include_directories(${PROJECT_SOURCE_DIR}/src/raftRpcPro/include)
include_directories(${CMAKE_BINARY_DIR}/src/raftRpcPro/generated)
include_directories(${PROJECT_SOURCE_DIR}/src/raftClerk/include)
include_directories(${PROJECT_SOURCE_DIR}/src/skipList/include)


# 设置项目库文件搜索路径 -L
link_directories(${PROJECT_SOURCE_DIR}/lib)

# src包含了所有的相关代码
add_subdirectory(src)
# example包含了使用的示例代码
add_subdirectory(example)
add_subdirectory(test)

add_library(skip_list_on_raft STATIC ${src_common} ${src_raftCore} ${src_raftRpcPro})
add_dependencies(skip_list_on_raft generate_raft_rpc_protos)
target_link_libraries(skip_list_on_raft muduo_net muduo_base pthread dl gRPC::grpc++ protobuf::libprotobuf boost_serialization)
# 添加格式化目标 start
# from : https://blog.csdn.net/guotianqing/article/details/121661067

add_custom_target(format
        COMMAND bash ${PROJECT_SOURCE_DIR}/format.sh
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        COMMAND echo "format done!"
)


# 添加格式化目标 end
