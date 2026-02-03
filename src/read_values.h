#pragma once

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <unistd.h>

#include "spdlog/spdlog.h"

constexpr uint32_t BLOCK_ALIGN = 4096;

struct ValueReader {
    int fd;
    uint32_t value_length;
    void *current_chunk;
    uint64_t file_offset;
    uint64_t chunk_offset;
    uint64_t chunk_size;

    ValueReader(int fd, uint32_t value_length, uint64_t start_offset) {
        this->fd = fd;
        this->value_length = value_length;
        chunk_size = value_length * BLOCK_ALIGN * 3;        // Avoid boundary conditions

        int res = posix_memalign(&current_chunk, BLOCK_ALIGN, chunk_size);
        assert(res == 0);
        uint64_t block_aligned_offset = ((start_offset * value_length) / chunk_size) * chunk_size;
        res = pread64(fd, current_chunk, chunk_size, block_aligned_offset);
        assert(res >= 0);
        file_offset = block_aligned_offset + chunk_size;

        chunk_offset = (start_offset * value_length) - block_aligned_offset;
        assert(chunk_offset % value_length == 0);
    }

    FORCEINLINE void *read_next() {
        if (chunk_offset == chunk_size) {
            int res = pread64(fd, current_chunk, chunk_size, file_offset);
            assert(res >= 0);
            file_offset += chunk_size;
            chunk_offset = 0;
        }
        uint8_t* res = (uint8_t*)current_chunk + chunk_offset;
        chunk_offset += value_length;
        return res;
    }
};