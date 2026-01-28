cmake_minimum_required(VERSION 3.10)
project(high_perf_sorting)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-format -Wno-register -Wno-unused-variable -Wno-unused-parameter \
    -Wno-ignored-attributes")

# Set optimization flags
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native -mtune=native")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")

# Set default build type to Release for better performance
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

find_package(OpenMP REQUIRED)

# Add executable
add_executable(sorter main.cpp)

# target_compile_options(sorter PRIVATE -fopenmp)
target_link_libraries(sorter PUBLIC OpenMP::OpenMP_CXX)

# Enable additional optimizations
# target_compile_options(sorter PRIVATE -march=native -mtune=native)
target_compile_options(sorter PRIVATE -mavx512f -mavx512vl)

target_link_libraries(sorter PRIVATE ips4o atomic tbb)