cmake_minimum_required(VERSION 3.16)
project(ctraderplus_cpp CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# ---- Dependencies ---------------------------------------------------------
# Drogon brings in trantor, jsoncpp, and (optionally) postgres/redis clients.
find_package(Drogon CONFIG REQUIRED)
find_package(Protobuf REQUIRED)
find_package(OpenSSL REQUIRED)

# jwt-cpp is header-only. Either install it system-wide or vendor it under
# third_party/jwt-cpp/include. We try find_package first, then a vendored path.
find_package(jwt-cpp CONFIG QUIET)
if(NOT jwt-cpp_FOUND)
  set(JWT_CPP_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/third_party/jwt-cpp/include")
endif()

# ---- Protobuf code generation --------------------------------------------
set(PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto")
file(GLOB PROTO_FILES "${PROTO_DIR}/*.proto")

set(PROTO_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

set(PROTO_SRCS "")
set(PROTO_HDRS "")
foreach(proto ${PROTO_FILES})
  get_filename_component(proto_name ${proto} NAME_WE)
  set(gen_src "${PROTO_GEN_DIR}/${proto_name}.pb.cc")
  set(gen_hdr "${PROTO_GEN_DIR}/${proto_name}.pb.h")
  add_custom_command(
    OUTPUT ${gen_src} ${gen_hdr}
    COMMAND protobuf::protoc
    ARGS --cpp_out=${PROTO_GEN_DIR} -I${PROTO_DIR} ${proto}
    DEPENDS ${proto}
    COMMENT "Generating C++ for ${proto_name}.proto"
    VERBATIM)
  list(APPEND PROTO_SRCS ${gen_src})
  list(APPEND PROTO_HDRS ${gen_hdr})
endforeach()

# ---- Application sources --------------------------------------------------
file(GLOB_RECURSE APP_SOURCES CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
list(REMOVE_ITEM APP_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/main.cc")

# Listing PROTO_HDRS as target sources forces protoc generation before any
# translation unit that includes the generated headers is compiled.
add_library(ctraderplus_lib STATIC ${APP_SOURCES} ${PROTO_SRCS} ${PROTO_HDRS})
target_include_directories(ctraderplus_lib PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/src"
  "${PROTO_GEN_DIR}")
if(JWT_CPP_INCLUDE)
  target_include_directories(ctraderplus_lib PUBLIC "${JWT_CPP_INCLUDE}")
endif()
target_link_libraries(ctraderplus_lib PUBLIC
  Drogon::Drogon
  protobuf::libprotobuf
  OpenSSL::SSL
  OpenSSL::Crypto)
if(jwt-cpp_FOUND)
  target_link_libraries(ctraderplus_lib PUBLIC jwt-cpp::jwt-cpp)
endif()

add_executable(ctraderplus_server "${CMAKE_CURRENT_SOURCE_DIR}/src/main.cc")
# Preserve Drogon WebSocket controller auto-registration symbols from the
# static library (otherwise the linker may drop the self-registering objects).
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_link_libraries(ctraderplus_server PRIVATE
    -Wl,--whole-archive ctraderplus_lib -Wl,--no-whole-archive)
else()
  target_link_libraries(ctraderplus_server PRIVATE ctraderplus_lib)
endif()

# ---- Tests ----------------------------------------------------------------
option(BUILD_TESTING "Build unit tests" ON)
if(BUILD_TESTING)
  enable_testing()
  file(GLOB TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cc")
  if(TEST_SOURCES)
    add_executable(ctraderplus_tests ${TEST_SOURCES})
    target_link_libraries(ctraderplus_tests PRIVATE ctraderplus_lib)
    add_test(NAME ctraderplus_tests COMMAND ctraderplus_tests)
  endif()
endif()
