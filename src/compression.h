#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <vector>

#include "key_value_pair.h"

namespace compression {

inline uint8_t bits_needed(uint64_t max_val) {
    if (max_val == 0) return 0;
    return static_cast<uint8_t>(64 - __builtin_clzll(max_val));
}

inline void pack_bits(const uint64_t* values, uint64_t count, uint8_t bit_width,
                      std::vector<uint8_t>& out) {
    if (bit_width == 0 || count == 0) {
        out.clear();
        return;
    }
    uint64_t total_bits = static_cast<uint64_t>(bit_width) * count;
    uint64_t total_bytes = (total_bits + 7) / 8;
    out.assign(total_bytes, 0);

    uint64_t bit_pos = 0;
    for (uint64_t i = 0; i < count; i++) {
        uint64_t val = values[i];
        uint8_t remaining = bit_width;
        while (remaining > 0) {
            uint64_t byte_idx = bit_pos / 8;
            uint8_t bit_offset = bit_pos % 8;
            uint8_t space = 8 - bit_offset;
            uint8_t to_write = (remaining < space) ? remaining : space;
            uint8_t bits = static_cast<uint8_t>(val & ((1u << to_write) - 1));
            out[byte_idx] |= static_cast<uint8_t>(bits << bit_offset);
            val >>= to_write;
            bit_pos += to_write;
            remaining -= to_write;
        }
    }
}

inline void unpack_bits(const uint8_t* packed, uint64_t count, uint8_t bit_width,
                        uint64_t* out) {
    if (bit_width == 0 || count == 0) {
        for (uint64_t i = 0; i < count; i++) out[i] = 0;
        return;
    }

    uint64_t bit_pos = 0;
    for (uint64_t i = 0; i < count; i++) {
        uint64_t val = 0;
        uint8_t remaining = bit_width;
        uint8_t shift = 0;
        while (remaining > 0) {
            uint64_t byte_idx = bit_pos / 8;
            uint8_t bit_offset = bit_pos % 8;
            uint8_t available = 8 - bit_offset;
            uint8_t to_read = (remaining < available) ? remaining : available;
            uint8_t bits = (packed[byte_idx] >> bit_offset) & ((1u << to_read) - 1);
            val |= static_cast<uint64_t>(bits) << shift;
            bit_pos += to_read;
            shift += to_read;
            remaining -= to_read;
        }
        out[i] = val;
    }
}

} // namespace compression

struct CompressedKeyIndex {
    struct KeyChunk {
        int64_t base_key;
        uint8_t delta_bit_width;
        uint64_t count;
        std::vector<uint8_t> packed_deltas;
    };

    uint64_t chunk_size;
    uint64_t num_elements;

    uint8_t index_bit_width;
    std::vector<uint8_t> packed_indexes;

    std::vector<KeyChunk> key_chunks;

    CompressedKeyIndex(const KeyValuePair<8, 8>* data,
                       uint64_t num_elements,
                       uint64_t chunk_size)
        : chunk_size(chunk_size), num_elements(num_elements),
          index_bit_width(0) {
        assert(num_elements > 0);
        assert(chunk_size > 0);

        // --- Compress indexes via bit-packing ---
        uint64_t max_index = 0;
        for (uint64_t i = 0; i < num_elements; i++) {
            if (data[i].value > max_index) max_index = data[i].value;
        }
        index_bit_width = compression::bits_needed(max_index);

        std::vector<uint64_t> indexes(num_elements);
        for (uint64_t i = 0; i < num_elements; i++) {
            indexes[i] = data[i].value;
        }
        compression::pack_bits(indexes.data(), num_elements, index_bit_width, packed_indexes);

        // --- Compress keys via chunked delta encoding ---
        uint64_t num_chunks = (num_elements + chunk_size - 1) / chunk_size;
        key_chunks.resize(num_chunks);

        for (uint64_t c = 0; c < num_chunks; c++) {
            uint64_t start = c * chunk_size;
            uint64_t end = start + chunk_size;
            if (end > num_elements) end = num_elements;
            uint64_t count = end - start;

            KeyChunk& chunk = key_chunks[c];
            chunk.base_key = data[start].key;
            chunk.count = count;

            if (count <= 1) {
                chunk.delta_bit_width = 0;
                chunk.packed_deltas.clear();
                continue;
            }

            uint64_t num_deltas = count - 1;
            std::vector<uint64_t> deltas(num_deltas);
            uint64_t max_delta = 0;
            for (uint64_t i = 0; i < num_deltas; i++) {
                int64_t d = data[start + i + 1].key - data[start + i].key;
                assert(d >= 0);
                deltas[i] = static_cast<uint64_t>(d);
                if (deltas[i] > max_delta) max_delta = deltas[i];
            }

            chunk.delta_bit_width = compression::bits_needed(max_delta);
            compression::pack_bits(deltas.data(), num_deltas, chunk.delta_bit_width,
                                   chunk.packed_deltas);
        }
    }

    void decompress(KeyValuePair<8, 8>* output) const {
        // --- Decompress indexes ---
        std::vector<uint64_t> indexes(num_elements);
        compression::unpack_bits(packed_indexes.data(), num_elements, index_bit_width,
                                 indexes.data());

        // --- Decompress keys ---
        uint64_t pos = 0;
        for (const auto& chunk : key_chunks) {
            output[pos].key = chunk.base_key;
            output[pos].value = indexes[pos];
            pos++;

            if (chunk.count <= 1) continue;

            uint64_t num_deltas = chunk.count - 1;
            std::vector<uint64_t> deltas(num_deltas);
            compression::unpack_bits(chunk.packed_deltas.data(), num_deltas,
                                     chunk.delta_bit_width, deltas.data());

            int64_t prev_key = chunk.base_key;
            for (uint64_t i = 0; i < num_deltas; i++) {
                prev_key += static_cast<int64_t>(deltas[i]);
                output[pos].key = prev_key;
                output[pos].value = indexes[pos];
                pos++;
            }
        }
        assert(pos == num_elements);
    }

    double average_delta_bit_width() const {
        if (key_chunks.empty()) return 0.0;
        uint64_t sum = 0;
        for (const auto& chunk : key_chunks) {
            sum += chunk.delta_bit_width;
        }
        return static_cast<double>(sum) / key_chunks.size();
    }

    uint64_t compressed_keys_size_bytes() const {
        uint64_t size = 0;
        for (const auto& chunk : key_chunks) {
            size += sizeof(chunk.base_key) + sizeof(chunk.delta_bit_width) + sizeof(chunk.count);
            size += chunk.packed_deltas.size();
        }
        return size;
    }

    uint64_t compressed_indexes_size_bytes() const {
        return packed_indexes.size() + sizeof(index_bit_width);
    }

    uint64_t compressed_size_bytes() const {
        return sizeof(chunk_size) + sizeof(num_elements)
             + compressed_keys_size_bytes()
             + compressed_indexes_size_bytes();
    }
};
