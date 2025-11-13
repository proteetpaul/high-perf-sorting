if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

add_executable(merge merge.cpp)

# Set compile options for the target
target_compile_options(merge PRIVATE -O3)

add_subdirectory(multi_way_merge)