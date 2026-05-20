set(PROTO_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/raftRPC.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/kvServerRPC.proto
)

set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_DIR})

set(GENERATED_SRCS)
set(GENERATED_HDRS)

foreach(PROTO_FILE ${PROTO_FILES})
  get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
  set(PROTO_SRC ${GENERATED_DIR}/${PROTO_NAME}.pb.cc)
  set(PROTO_HDR ${GENERATED_DIR}/${PROTO_NAME}.pb.h)
  set(GRPC_SRC ${GENERATED_DIR}/${PROTO_NAME}.grpc.pb.cc)
  set(GRPC_HDR ${GENERATED_DIR}/${PROTO_NAME}.grpc.pb.h)

  # cpp 与 grpc 分两步生成；proto 须含 option cc_generic_services = false;
  add_custom_command(
      OUTPUT ${PROTO_SRC} ${PROTO_HDR} ${GRPC_SRC} ${GRPC_HDR}
      COMMAND $<TARGET_FILE:protobuf::protoc>
          --cpp_out=${GENERATED_DIR}
          -I${CMAKE_CURRENT_SOURCE_DIR}
          ${PROTO_FILE}
      COMMAND $<TARGET_FILE:protobuf::protoc>
          --grpc_out=${GENERATED_DIR}
          --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
          -I${CMAKE_CURRENT_SOURCE_DIR}
          ${PROTO_FILE}
      DEPENDS ${PROTO_FILE}
      VERBATIM
  )

  list(APPEND GENERATED_SRCS ${PROTO_SRC} ${GRPC_SRC})
  list(APPEND GENERATED_HDRS ${PROTO_HDR} ${GRPC_HDR})
endforeach()

add_custom_target(generate_raft_rpc_protos DEPENDS ${GENERATED_SRCS} ${GENERATED_HDRS})

set(src_raftRpcPro ${GENERATED_SRCS} CACHE INTERNAL "Raft/KV proto generated sources")
set(src_raftRpcPro_headers ${GENERATED_HDRS} CACHE INTERNAL "Raft/KV proto generated headers")
