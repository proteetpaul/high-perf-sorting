#pragma once
#include <cstdint>
#include <string>

struct Config {
    static constexpr uint32_t BLOCK_SIZE_ALIGN = 4096;

    uint32_t num_threads;

    uint64_t run_size_bytes;        

    uint64_t file_size_bytes;

    uint32_t merge_read_chunk_size;     // Chunk size for reading from the intermediate sorted runs

    uint32_t merge_write_chunk_size;    // Chunk size for writing to the output file

    std::string input_file;

    std::string output_file;

    std::string intermediate_file_prefix;

    inline uint64_t num_runs() {
        return (file_size_bytes + run_size_bytes - 1) / run_size_bytes;
    }
};