#pragma once
#include <cstdint>
#include <string>

struct Config {
    static constexpr uint32_t BLOCK_SIZE_ALIGN = 4096;

    uint32_t num_threads;

    // Number of threads to use for the post-merge reconciliation of keys and values 
    uint32_t num_threads_post_merge;

    uint64_t run_size_bytes;        

    uint64_t file_size_bytes;

    std::string input_file;

    std::string output_file;

    std::string intermediate_file_prefix;

    bool separate_values;

    bool use_std_sort;

    /** If true, use io_uring for phase 1+2 (read + extract keys). Requires liburing. */
    bool use_async{false};

    bool benchmark_compression{false};
    uint64_t compression_chunk_size{256};

    inline uint64_t num_runs() {
        return (file_size_bytes + run_size_bytes - 1) / run_size_bytes;
    }
};