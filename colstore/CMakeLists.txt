# colstore: columnar analytics store. Owned by the colstore track.
#
# Sources split into a static library (format, writer, reader) and the CLI
# (main.cpp) so the unit tests link the exact code the binary ships.
file(GLOB COLSTORE_LIB_SOURCES CONFIGURE_DEPENDS src/*.cpp)
list(FILTER COLSTORE_LIB_SOURCES EXCLUDE REGEX "main\\.cpp$")

add_library(colstore_core STATIC ${COLSTORE_LIB_SOURCES})
target_link_libraries(colstore_core PUBLIC adstack_common)
target_include_directories(colstore_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(colstore src/main.cpp)
target_link_libraries(colstore PRIVATE colstore_core)

if(ADSTACK_BUILD_TESTS)
  file(GLOB COLSTORE_TEST_SOURCES CONFIGURE_DEPENDS tests/*.cpp)
  if(COLSTORE_TEST_SOURCES)
    add_executable(colstore_tests ${COLSTORE_TEST_SOURCES})
    target_link_libraries(colstore_tests PRIVATE colstore_core GTest::gtest_main)
    gtest_discover_tests(colstore_tests)
  endif()
endif()
