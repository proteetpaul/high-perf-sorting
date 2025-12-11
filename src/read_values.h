#pragma once

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <unistd.h>

constexpr uint32_t BLOCK_ALIGN = 4096;

constexpr uint32_t CHUNK_SIZE = 1 << 22; // 4MB

struct ValueReader {
    int fd;
    uint32_t value_length;
    void *current_chunk;
    uint64_t file_offset;
    uint64_t chunk_offset;

    ValueReader(int fd, uint32_t value_length, uint64_t start_offset) {
        this->fd = fd;
        this->value_length = value_length;

        int res = posix_memalign(&current_chunk, BLOCK_ALIGN, CHUNK_SIZE);
        assert(res == 0);
        uint64_t block_aligned_offset = (start_offset * value_length) / BLOCK_ALIGN * BLOCK_ALIGN;
        res = pread64(fd, current_chunk, CHUNK_SIZE, block_aligned_offset);
        assert(res >= 0);
        file_offset = block_aligned_offset + CHUNK_SIZE;

        chunk_offset = start_offset - block_aligned_offset;
    }

    void *read_next() {
        if (chunk_offset == CHUNK_SIZE) {
            int res = pread64(fd, current_chunk, CHUNK_SIZE, file_offset);
            assert(res >= 0);
            file_offset += CHUNK_SIZE;
            chunk_offset = 0;
        }
        uint8_t* res = (uint8_t*)current_chunk + chunk_offset;
        chunk_offset += value_length;
        return res;
    }
};