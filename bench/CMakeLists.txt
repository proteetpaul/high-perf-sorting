# Set default build type to Release for better performance
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

add_executable(merge merge.cpp)

# Set compile options for the target
target_compile_options(merge PRIVATE -O3)
