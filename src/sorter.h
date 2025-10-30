#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <iomanip>

#include "key_index_pair.h"
#include "sorted_run.h"
#include "config.h"
#include "merge_tree.h"
#define _REENTRANT
#include "ips4o.hpp"

struct TimingInfo {
    float intermediate_write;
    float output_write;
    float merge_read;
    float input_read;
    float sort_time;
};

template <uint32_t KeyLength, uint32_t ValueLength>
class Sorter {
    static constexpr uint32_t ELEM_SIZE = sizeof(KeyValuePair<KeyLength, ValueLength>);
    Config config;

    int read_fd; // File from which input records are read
    int write_fd; // File to which sorted records are written

    TimingInfo timing_info;
    uint64_t output_file_offset;

    std::vector<KeyValuePair<KeyLength, ValueLength>> read_input_chunk(uint64_t chunk_id);

    SortedRun create_disk_run(std::vector<KeyValuePair<KeyLength, ValueLength>> &vec, uint64_t chunk_idx);

    void write_output_chunk(void *buffer, uint64_t length);

    void in_place_sort(std::vector<KeyValuePair<KeyLength, ValueLength>> &vec);

    void merge(std::vector<SortedRun> &sorted_runs);

public:
    Sorter(Config &&config) {
        this->config = std::move(config);
        timing_info = {0, 0, 0, 0, 0};
        read_fd = -1;
        write_fd = -1;
        output_file_offset = 0ll;
    }

    void sort();

    void print_timing_stats() {
        printf("Read input: %0.2f ms\n", timing_info.input_read);
        printf("Read merge phase: %0.2f ms\n", timing_info.merge_read);
        printf("Write sorted runs: %0.2f ms\n", timing_info.intermediate_write);
        printf("Write output: %0.2f ms\n", timing_info.output_write);
        printf("Sorting time: %02.f ms\n", timing_info.sort_time);
    }
};

template <uint32_t KeyLength, uint32_t ValueLength>
void Sorter<KeyLength, ValueLength>::in_place_sort(std::vector<KeyValuePair<KeyLength, ValueLength>> &vec) {
    auto start = std::chrono::high_resolution_clock::now();
    ips4o::parallel::sort(vec.begin(), vec.end(), std::less<KeyValuePair<KeyLength, ValueLength>>{}, config.num_threads);
    auto end = std::chrono::high_resolution_clock::now();

    timing_info.sort_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;
}

template <uint32_t KeyLength, uint32_t ValueLength>
void Sorter<KeyLength, ValueLength>::sort() {
    // Ensure that elements don't span across on-disk blocks
    assert(Config::BLOCK_SIZE_ALIGN % ELEM_SIZE == 0);

    read_fd = open(config.input_file.c_str(), O_RDONLY | O_DIRECT);
    write_fd = open(config.output_file.c_str(), O_CREAT | O_RDWR | O_TRUNC | O_DIRECT, 0644);

    uint64_t num_runs = config.num_runs();
    if (num_runs == 1) {
        auto v = read_input_chunk(0);
        in_place_sort(v);
        write_output_chunk(v.data(), config.file_size_bytes);
        return;
    }

    uint64_t memory_size_bytes = config.run_size_bytes;
    uint64_t merge_chunk_size_bytes = (memory_size_bytes / (num_runs + 1));
    // align this to 4096 bytes for direct IO
    merge_chunk_size_bytes = (merge_chunk_size_bytes / Config::BLOCK_SIZE_ALIGN) * Config::BLOCK_SIZE_ALIGN;
    config.merge_read_chunk_size = merge_chunk_size_bytes;
    config.merge_write_chunk_size = merge_chunk_size_bytes;

    std::cout << "Number of runs: " << num_runs << "\n";
    std::cout << "Chunk size for merge step: " << merge_chunk_size_bytes << "\n";

    std::vector<SortedRun> sorted_runs;

    for (int i=0; i<num_runs; i++) {
        auto v = read_input_chunk(i);
        in_place_sort(v);
        sorted_runs.push_back(create_disk_run(v, i));    
    }
    merge(sorted_runs);
}

template <uint32_t KeyLength, uint32_t ValueLength>
std::vector<KeyValuePair<KeyLength, ValueLength>> Sorter<KeyLength, ValueLength>::read_input_chunk(uint64_t chunk_id) {
    std::vector<KeyValuePair<KeyLength, ValueLength>> output_vector(config.run_size_bytes / ELEM_SIZE);
    
    auto start = std::chrono::high_resolution_clock::now();
    // TODO(): Fix this
    auto ret = pread64(read_fd, output_vector.data(), config.run_size_bytes, chunk_id * config.run_size_bytes);
    assert(ret == config.run_size_bytes);
    auto end = std::chrono::high_resolution_clock::now();
    
    timing_info.input_read += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;
    return output_vector;
}

template <uint32_t KeyLength, uint32_t ValueLength>
SortedRun Sorter<KeyLength, ValueLength>::create_disk_run(std::vector<KeyValuePair<KeyLength, ValueLength>> &vec, uint64_t chunk_idx) {
    std::string file_name = config.intermediate_file_prefix + "-chunk-" + std::to_string(chunk_idx);
    int fd = open(file_name.c_str(), O_CREAT | O_RDWR | O_TRUNC | O_DIRECT, 0644);

    uint64_t output_chunk_size = vec.size() * ELEM_SIZE;
    auto start = std::chrono::high_resolution_clock::now();

    auto ret = pwrite64(fd, vec.data(), output_chunk_size, 0);
    assert(ret == output_chunk_size);
    auto end = std::chrono::high_resolution_clock::now();

    timing_info.intermediate_write += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;
    return SortedRun {fd, vec.size()};
}

template <uint32_t KeyLength, uint32_t ValueLength>
void Sorter<KeyLength, ValueLength>::merge(std::vector<SortedRun> &sorted_runs) {
    using RecordType = KeyValuePair<KeyLength, ValueLength>;
    printf("Starting merge step...\n");
    auto start = std::chrono::high_resolution_clock::now();

    uint64_t num_runs = config.num_runs();
    std::vector<std::shared_ptr<SortedRunReader<RecordType>>> readers;
    for (auto& run: sorted_runs) {
        auto reader = new SortedRunReader<RecordType>(config.merge_read_chunk_size, run);
        readers.push_back(std::make_shared<>(reader));
    }
    void *write_buffer; 
    int ret = posix_memalign(&write_buffer, 4096, config.merge_write_chunk_size);
    assert(ret == 0);
    uint64_t write_buffer_offset = 0;
    uint64_t file_write_offset = 0;

    MergeTree<RecordType> merge_tree {readers};
    auto inf_record = RecordType::inf();
    while (true) {
        auto top_record = merge_tree.pop();
        if (top_record == inf_record) {
            // Merge is complete when the topmost record is invalid
            break;
        }
        std::memcpy(reinterpret_cast<uint8_t*>(write_buffer) + write_buffer_offset, 
            reinterpret_cast<uint8_t*>(top_record), ELEM_SIZE);
        write_buffer_offset += ELEM_SIZE;
        if (write_buffer_offset == config.merge_write_chunk_size) {
            write_output_chunk(write_buffer, config.merge_write_chunk_size);
            write_buffer_offset = 0ll;
        }
    }

    // while (readers.size()) {
    //     auto start = std::chrono::high_resolution_clock::now();
    //     KeyValuePair<KeyLength, ValueLength> *smallest = readers[0]->next();
    //     uint32_t smallest_run_idx = 0;
    //     for (uint32_t i=1; i<readers.size(); i++) {
    //         KeyValuePair<KeyLength, ValueLength> *next = readers[i]->next();
    //         if (*next < *smallest) {
    //             smallest = next;
    //             smallest_run_idx = i;
    //         }
    //     }
        
    //     auto end = std::chrono::high_resolution_clock::now();
    //     timing_info.merge_read += std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() / 1000.0;
        
    //     readers[smallest_run_idx]->advance();
    //     if (!readers[smallest_run_idx]->has_next()) {
    //         readers.erase(readers.begin() + smallest_run_idx);
    //     }
    //     std::memcpy(reinterpret_cast<uint8_t*>(write_buffer) + write_buffer_offset, 
    //         reinterpret_cast<uint8_t*>(smallest), ELEM_SIZE);

    //     write_buffer_offset += ELEM_SIZE;
    //     if (write_buffer_offset == config.merge_write_chunk_size) {
    //         write_output_chunk(write_buffer, config.merge_write_chunk_size);
    //         write_buffer_offset = 0ll;
    //     }
    // }
    if (write_buffer_offset > 0) {
        auto start = std::chrono::high_resolution_clock::now();
        write_output_chunk(write_buffer, write_buffer_offset);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto merge_duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "Total merge time: " << std::fixed << std::setprecision(3) 
            << merge_duration.count() / 1000.0 << " ms\n";
}

template <uint32_t KeyLength, uint32_t ValueLength>
void Sorter<KeyLength, ValueLength>::write_output_chunk(void *buffer, uint64_t length) {
    auto start = std::chrono::high_resolution_clock::now();
    int ret = pwrite64(write_fd, buffer, length, output_file_offset);
    assert(ret == length);
    auto end = std::chrono::high_resolution_clock::now();

    timing_info.output_write += std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() / 1000.0;
    output_file_offset += length;
}