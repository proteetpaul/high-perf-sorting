#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <chrono>
#include <deque>
#include <unistd.h>
#include <vector>

// #include "io_uring.h"

struct SortedRun {
    int fd;
    uint64_t num_elements;
};

enum ReaderState {
    WaitingForIO,
    Ready,
};

template <typename RecordType>
class SortedRunReader {
    static constexpr uint32_t ELEM_SIZE = sizeof(RecordType);

    SortedRun run;

    uint64_t file_offset;

    void *buffer;

    uint64_t processed;             // Total number of elements processed for this run

    uint64_t buffer_size;           // Size of reads from disk

    uint64_t buffer_offset;

    uint64_t read_time;

    // std::vector<void*> all_buffers;

    // std::deque<uint32_t> ready_for_io;

    // std::vector<uint32_t> chunk_to_buffer_idx_mapping;

    // uint32_t current_chunk;

    // uint32_t next_chunk;

    // ReaderState state;
public:
    // IoTask* get_next_io_task() {

    // }

    // void io_complete(IoTask *task) {

    // }

    SortedRunReader(uint64_t read_chunk_size_bytes, SortedRun run)
            : buffer_size(read_chunk_size_bytes), run(run) {
        processed = 0;
        buffer_offset = read_chunk_size_bytes;
        file_offset = 0;
        read_time = 0;
        int ret = posix_memalign(&buffer, 4096, buffer_size);
        assert(ret == 0);
    }

    bool has_next() {
        return processed < run.num_elements;
    }

    RecordType next() {
        if (processed == run.num_elements) {
            return RecordType::inf();
        }
        if (buffer_offset + ELEM_SIZE > buffer_size) {
            // read next chunk from disk
            uint64_t remaining_bytes_to_read = (run.num_elements - processed) * ELEM_SIZE;
            uint64_t bytes_to_read = std::min(buffer_size, remaining_bytes_to_read);
            auto start = std::chrono::high_resolution_clock::now();
            int ret = pread64(run.fd, buffer, bytes_to_read, file_offset);
            auto end = std::chrono::high_resolution_clock::now();
            read_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
            assert(ret == bytes_to_read);
            file_offset += bytes_to_read;
            buffer_offset = 0ll;
        }
        char *buffer_ptr = (char*)buffer + buffer_offset;
        processed++;
        buffer_offset += ELEM_SIZE;
        return RecordType::from_ptr(buffer_ptr);
    }

    void advance() {
        processed++;
        buffer_offset += ELEM_SIZE;
    }

    ~SortedRunReader() {
        free(buffer);
    }

    uint64_t get_file_read_time() {
        return read_time;
    }
};