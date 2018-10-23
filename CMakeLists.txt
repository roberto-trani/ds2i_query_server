cmake_minimum_required(VERSION 2.8)
project(QUERY_SERVER)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release")
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
endif ()

if (UNIX)
   # C++11
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

   # For hardware popcount
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")

   # Extensive warnings
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-braces")
   # Silence a warning bug in Boost
   if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
     set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-local-typedefs")
   endif ()
   # set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wconversion")

   set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -ggdb") # Add debug info anyway
endif()

find_package(Boost REQUIRED COMPONENTS iostreams unit_test_framework system thread)
include_directories(${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARY_DIRS})


find_package( ZLIB REQUIRED )
include_directories( ${ZLIB_INCLUDE_DIRS} )

# OPEN MP inclusion
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fopenmp")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")

# add the root directory to include path to make includes absolute
include_directories(${QUERY_SERVER_SOURCE_DIR})
include_directories(${QUERY_SERVER_SOURCE_DIR}/ds2i/)

include_directories( ds2i )
add_subdirectory(${QUERY_SERVER_SOURCE_DIR}/ds2i/FastPFor EXCLUDE_FROM_ALL)

file(GLOB query_SRC
    "query/*.hpp"
    "query_server/*.hpp"
)

add_executable(query_server query_server.cpp ${query_SRC})
target_link_libraries(query_server
    ${Boost_LIBRARIES}
    FastPFor_lib
    pthread
)
