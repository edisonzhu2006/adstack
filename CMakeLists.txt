cmake_minimum_required(VERSION 3.24)
project(adstack LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# All binaries in build/bin so subdirectory object dirs cannot shadow them.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

# ADSTACK_SANITIZE: none | address | thread
set(ADSTACK_SANITIZE "none" CACHE STRING "Sanitizer to enable")
option(ADSTACK_BUILD_TESTS "Build unit tests (fetches GoogleTest)" ON)
option(ADSTACK_BUILD_ADAPTERS "Build real-data adapters (fetches nlohmann/json)" ON)

add_compile_options(-Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wno-sign-conversion)

if(ADSTACK_SANITIZE STREQUAL "address")
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=address,undefined)
elseif(ADSTACK_SANITIZE STREQUAL "thread")
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=thread)
endif()

include(FetchContent)

# ---- common -------------------------------------------------------------
add_library(adstack_common
  common/event.cpp
  common/args.cpp
)
target_include_directories(adstack_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# ---- tools --------------------------------------------------------------
add_executable(gen tools/gen.cpp)
target_link_libraries(gen PRIVATE adstack_common)

add_executable(oracle tools/oracle.cpp)
target_link_libraries(oracle PRIVATE adstack_common)

add_executable(evdump tools/evdump.cpp)
target_link_libraries(evdump PRIVATE adstack_common)


# ---- adapters -----------------------------------------------------------
if(ADSTACK_BUILD_ADAPTERS)
  add_executable(criteo2events adapters/criteo2events.cpp)
  target_link_libraries(criteo2events PRIVATE adstack_common)

  FetchContent_Declare(nlohmann_json
    URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
  FetchContent_MakeAvailable(nlohmann_json)

  add_executable(wiki2events adapters/wiki2events.cpp)
  target_link_libraries(wiki2events PRIVATE adstack_common nlohmann_json::nlohmann_json)
endif()

# ---- tests --------------------------------------------------------------
if(ADSTACK_BUILD_TESTS)
  FetchContent_Declare(googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  enable_testing()
  add_executable(adstack_tests
    tests/event_test.cpp
    tests/window_test.cpp
  )
  target_link_libraries(adstack_tests PRIVATE adstack_common GTest::gtest_main)
  include(GoogleTest)
  gtest_discover_tests(adstack_tests)
endif()

# ---- projects (each owns its own CMakeLists so tracks do not collide) -----
add_subdirectory(streamkit)
add_subdirectory(colstore)
