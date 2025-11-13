if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

add_executable(multi_way_merge merge.cpp)

target_compile_options(multi_way_merge PRIVATE -O3)
target_include_directories(multi_way_merge PUBLIC spdlog/include)