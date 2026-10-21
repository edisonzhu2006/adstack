# colstore: columnar analytics store. Owned by the colstore track.
#
# Sources split into a static library (format, writer, reader) and the CLI
# (main.cpp) so the unit tests link the exact code the binary ships. The
# SQLite benchmark loader (m5) is kept out of both: it is the only code here
# that needs a third-party library, so it is its own library and binary,
# built only when sqlite3 is found (macOS ships it in the SDK; a Linux CI
# image without libsqlite3-dev still builds everything else).
file(GLOB COLSTORE_LIB_SOURCES CONFIGURE_DEPENDS src/*.cpp)
list(FILTER COLSTORE_LIB_SOURCES EXCLUDE REGEX "main\\.cpp$|sqlite")

add_library(colstore_core STATIC ${COLSTORE_LIB_SOURCES})
target_link_libraries(colstore_core PUBLIC adstack_common)
target_include_directories(colstore_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(colstore src/main.cpp)
target_link_libraries(colstore PRIVATE colstore_core)

# ---- SQLite comparison (benchmark only, optional) --------------------------
find_path(SQLITE3_INCLUDE_DIR sqlite3.h)
find_library(SQLITE3_LIBRARY sqlite3)
if(SQLITE3_INCLUDE_DIR AND SQLITE3_LIBRARY)
  set(COLSTORE_HAVE_SQLITE ON)
  message(STATUS "colstore: sqlite3 found (${SQLITE3_LIBRARY}); building colstore_sqlite")
  add_library(colstore_sqlite_loader STATIC src/sqlite_loader.cpp)
  target_link_libraries(colstore_sqlite_loader PUBLIC colstore_core ${SQLITE3_LIBRARY})
  # SYSTEM: the SDK header is not held to the project's warning flags.
  target_include_directories(colstore_sqlite_loader SYSTEM PUBLIC ${SQLITE3_INCLUDE_DIR})
  add_executable(colstore_sqlite src/bench_sqlite.cpp)
  target_link_libraries(colstore_sqlite PRIVATE colstore_sqlite_loader)
else()
  set(COLSTORE_HAVE_SQLITE OFF)
  message(STATUS "colstore: sqlite3 not found; skipping colstore_sqlite (bench/colstore_bench.sh m5 needs it)")
endif()

if(ADSTACK_BUILD_TESTS)
  file(GLOB COLSTORE_TEST_SOURCES CONFIGURE_DEPENDS tests/*.cpp)
  if(NOT COLSTORE_HAVE_SQLITE)
    list(FILTER COLSTORE_TEST_SOURCES EXCLUDE REGEX "sqlite")
  endif()
  if(COLSTORE_TEST_SOURCES)
    add_executable(colstore_tests ${COLSTORE_TEST_SOURCES})
    # The loader library links colstore_core PUBLIC, so naming both would
    # make ld warn about a duplicate library.
    if(COLSTORE_HAVE_SQLITE)
      target_link_libraries(colstore_tests PRIVATE colstore_sqlite_loader GTest::gtest_main)
    else()
      target_link_libraries(colstore_tests PRIVATE colstore_core GTest::gtest_main)
    endif()
    gtest_discover_tests(colstore_tests)
  endif()
endif()
