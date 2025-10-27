#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <unistd.h>

#include "key_index_pair.h"

struct SortedRun {
    int fd;
    uint64_t num_elements;
};

template <uint32_t KeyLength, uint32_t ValueLength>
class SortedRunReader {
    static constexpr uint32_t ELEM_SIZE = sizeof(KeyValuePair<KeyLength, ValueLength>);

    SortedRun run;

    uint64_t file_offset;

    void *buffer;

    uint64_t processed;             // Total number of elements processed for this run

    uint64_t buffer_size;           // Size of reads from disk

    uint64_t buffer_offset;
public:
    SortedRunReader(uint64_t read_chunk_size_bytes, SortedRun run)
            : buffer_size(read_chunk_size_bytes), run(run) {
        processed = 0;
        buffer_offset = read_chunk_size_bytes;
        file_offset = 0;
        int ret = posix_memalign(&buffer, 4096, buffer_size);
        assert(ret == 0);
    }

    bool has_next() {
        return processed < run.num_elements;
    }

    KeyValuePair<KeyLength, ValueLength> next() {
        if (buffer_offset + ELEM_SIZE > buffer_size) {
            // read next chunk from disk
            uint64_t remaining_bytes_to_read = (run.num_elements - processed) * ELEM_SIZE;
            uint64_t bytes_to_read = std::min(buffer_size, remaining_bytes_to_read);
            int ret = pread64(run.fd, buffer, bytes_to_read, file_offset);
            assert(ret == bytes_to_read);
            file_offset += bytes_to_read;
            buffer_offset = 0ll;
        }
        char *buffer_ptr = (char*)buffer + buffer_offset;
        KeyValuePair<KeyLength, ValueLength> k {buffer_ptr, reinterpret_cast<char*>(buffer_ptr + KeyLength)};
        return k;
    }

    void advance() {
        processed++;
        buffer_offset += ELEM_SIZE;
    }

    ~SortedRunReader() {
        free(buffer);
    }
};