# streamkit: streaming aggregation engine. Owned by the streamkit track.
#
# The engine's reusable pieces (dedup, the pipeline, the checkpoint code)
# live in a static library so the binary and the unit tests compile
# them once and link the same object code.
add_library(streamkit_core
  checkpoint.cpp
  dedup.cpp
  pipeline.cpp
)
find_package(Threads REQUIRED)
target_link_libraries(streamkit_core PUBLIC adstack_common Threads::Threads)
target_include_directories(streamkit_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(streamkit main.cpp)
target_link_libraries(streamkit PRIVATE streamkit_core)

if(ADSTACK_BUILD_TESTS)
  file(GLOB STREAMKIT_TEST_SOURCES CONFIGURE_DEPENDS tests/*.cpp)
  if(STREAMKIT_TEST_SOURCES)
    add_executable(streamkit_tests ${STREAMKIT_TEST_SOURCES})
    target_link_libraries(streamkit_tests PRIVATE streamkit_core GTest::gtest_main)
    gtest_discover_tests(streamkit_tests)
  endif()
endif()
