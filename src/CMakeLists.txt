cmake_minimum_required(VERSION 3.10)
project(high_perf_sorting)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Set optimization flags
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")

# Set default build type to Release for better performance
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Add executable
add_executable(sorter main.cpp io_backend.cpp reader.cpp)
find_package(TBB REQUIRED)

# Set compile options for the target
target_compile_options(sorter PRIVATE -O3)

# Enable additional optimizations
# target_compile_options(sorter PRIVATE -march=native -mtune=native)

target_link_libraries(sorter PRIVATE ips4o TBB::tbb uring pthread)