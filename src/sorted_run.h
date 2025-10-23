#pragma once

#include <cstdint>
#include <cstdlib>
#include <unistd.h>
#include <vector>

#include "key_index_pair.h"

template <uint32_t KeyLength, uint32_t ValueLength>
class SortedRun {
    uint64_t file_start_offset;

    uint64_t current_file_offset;

    int fd;

    void *buffer;

    uint64_t total_elements;        // Number of elements in a single run

    uint64_t remaining_in_mem;      // Number of elements in in-memory run

    uint64_t processed;             // Total number of elements processed for this run

    uint64_t read_chunk_size_bytes; // Size of reads from disk

    std::vector<KeyValuePair<KeyLength, ValueLength>> *in_memory_run;

public:
    SortedRun(uint64_t read_chunk_size_bytes, int fd, uint64_t file_start_offset, uint64_t run_size_bytes)
            : fd(fd), read_chunk_size_bytes(read_chunk_size_bytes), 
            file_start_offset(file_start_offset), current_file_offset(file_start_offset), 
            in_memory_run(nullptr) {
        remaining_in_mem = 0;
        processed = 0;
        total_elements = run_size_bytes / sizeof(KeyValuePair<KeyLength, ValueLength>);
        int ret = posix_memalign(&buffer, 4096, read_chunk_size_bytes);
    }

    SortedRun(uint64_t run_size_bytes, std::vector<KeyValuePair<KeyLength, ValueLength>> *in_memory_run)
            : in_memory_run(in_memory_run) {
        total_elements = run_size_bytes / sizeof(KeyValuePair<KeyLength, ValueLength>);
        processed = 0;
    }

    bool has_next() {
        return processed < total_elements;
    }

    KeyValuePair<KeyLength, ValueLength> get_next() {
        if (in_memory_run != nullptr) {
            return in_memory_run->at(processed);
        }
        if (!remaining_in_mem) {
            // read next chunk from disk
            uint64_t remaining_bytes_to_read = (total_elements - processed) * sizeof(KeyValuePair<KeyLength, ValueLength>);
            uint64_t bytes_to_read = std::min(read_chunk_size_bytes, remaining_bytes_to_read);
            int ret = pread(fd, buffer, bytes_to_read, current_file_offset);
            current_file_offset += bytes_to_read;
            remaining_in_mem = bytes_to_read / sizeof(KeyValuePair<KeyLength, ValueLength>);
        }
        uint64_t buffer_offset = read_chunk_size_bytes - remaining_in_mem * sizeof(KeyValuePair<KeyLength, ValueLength>);
        char *buffer_ptr = (char*)buffer + buffer_offset;
        KeyValuePair<KeyLength, ValueLength> k {buffer_ptr, reinterpret_cast<char*>(buffer_ptr + KeyLength)};
        return k;
    }

    void next() {
        processed++;
        remaining_in_mem--;
    }

    ~SortedRun() {
        free(buffer);
    }
};