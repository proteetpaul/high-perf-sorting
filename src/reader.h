#pragma once

#include "io_task.h"
#include "io_backend.h"

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <memory>
#include <unistd.h>
#include <vector>
#include <optional>

struct SortedRun {
    int fd;
    uint64_t num_elements;
};

enum ReaderState {
    WaitingForIO,
    Ready,
};

class SortedRunReader {
    static const uint32_t NUM_BUFFERS = 2;
    uint32_t elem_size;

    SortedRun run;

    uint64_t processed;             // Total number of elements processed for this run

    uint64_t chunk_size;           // Size of reads from disk

    void *current_buffer;

    uint64_t buffer_offset;

    std::vector<void*> all_buffers;

    std::deque<uint32_t> ready_for_io;

    std::vector<int32_t> chunk_to_buffer_idx_mapping;

    uint32_t current_chunk;

    uint32_t next_chunk;

    uint32_t num_chunks;

    ReaderState state;

    std::shared_ptr<UringHandler> io_handler;

public:
    SortedRunReader(uint64_t buffer_capacity, SortedRun run, int elem_size, 
        std::shared_ptr<UringHandler> handler);

    void submit_next_io_task();

    ReaderState get_state() {
        return state;
    }

    void io_complete(IoTask *task);

    void *next();

    bool has_next() {
        return processed < run.num_elements;
    }

    ~SortedRunReader() {
    }
};