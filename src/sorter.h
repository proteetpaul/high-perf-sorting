#pragma once

#include <cstdint>
#include <cstring>
#include <iostream>
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
#define _REENTRANT
#include "ips4o.hpp"

struct TimingInfo {
    float intermediate_write;
    float output_write;
    float merge_read;
    float input_read;
};

template <uint32_t KeyLength, uint32_t ValueLength>
class Sorter {
    Config config;

    int read_fd; // File from which input records are read
    int write_fd; // File to which sorted records are written
    int intermediate_fd; // File to store sorted runs

    TimingInfo timing_info;
    uint64_t write_offset;

    std::vector<KeyValuePair<KeyLength, ValueLength>> read_input_chunk(uint64_t chunk_id);

    void write_intermediate_buffer_to_disk(std::vector<KeyValuePair<KeyLength, ValueLength>> &vec, uint64_t chunk_idx);

    void write_output_chunk(void *buffer, uint64_t length);

    void in_place_sort(std::vector<KeyValuePair<KeyLength, ValueLength>> &vec);

    void merge(std::vector<KeyValuePair<KeyLength, ValueLength>> &last_run);

public:
    Sorter(Config &&config) {
        this->config = std::move(config);
        timing_info = {0, 0, 0, 0};
        read_fd = -1;
        write_fd = -1;
        intermediate_fd = -1;
        write_offset = 0ll;
    }

    void sort();

    void print_timing_stats() {
        printf("Read input: %0.2f ms\n", timing_info.input_read);
        printf("Read merge phase: %0.2f ms\n", timing_info.merge_read);
        printf("Write sorted runs: %0.2f ms\n", timing_info.intermediate_write);
        printf("Write output: %0.2f ms\n", timing_info.output_write);
    }
};

template <uint32_t KeyLength, uint32_t ValueLength>
void Sorter<KeyLength, ValueLength>::in_place_sort(std::vector<KeyValuePair<KeyLength, ValueLength>> &vec) {
    // if (config.num_threads == 1) {
    //     ips4o::sort(vec.begin(), vec.end(), std::less<KeyValuePair<KeyLength, ValueLength>>{});
    // } else {
    ips4o::parallel::sort(vec.begin(), vec.end(), std::less<KeyValuePair<KeyLength, ValueLength>>{}, config.num_threads);
    // }
}

template <uint32_t KeyLength, uint32_t ValueLength>
void Sorter<KeyLength, ValueLength>::sort() {
    read_fd = open(config.input_file.c_str(), O_RDONLY);
    intermediate_fd = open(config.intermediate_file.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    write_fd = open(config.output_file.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    posix_fadvise64(read_fd, 0, config.file_size_bytes, POSIX_FADV_SEQUENTIAL);
    posix_fadvise64(write_fd, 0, config.file_size_bytes, POSIX_FADV_SEQUENTIAL);

    std::vector<KeyValuePair<KeyLength, ValueLength>> last_run;

    uint64_t num_runs = config.num_runs();
    uint64_t memory_size_bytes = config.run_size_bytes;
    uint64_t read_chunk_size_bytes = (memory_size_bytes / (num_runs + 1));
    read_chunk_size_bytes = (read_chunk_size_bytes / sizeof(KeyValuePair<KeyLength, ValueLength>)) * sizeof(KeyValuePair<KeyLength, ValueLength>);
    config.merge_read_chunk_size = read_chunk_size_bytes;
    config.merge_write_chunk_size = read_chunk_size_bytes;

    std::cout << "Number of runs: " << num_runs << "\n";
    std::cout << "Chunk size for merge step: " << read_chunk_size_bytes << "\n";
    double total_sort_time = 0.0;

    for (int i=0; i<num_runs; i++) {
        auto start = std::chrono::high_resolution_clock::now();
        auto v = read_input_chunk(i);
        auto end = std::chrono::high_resolution_clock::now();
        timing_info.input_read += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;
        
        auto sort_start = std::chrono::high_resolution_clock::now();
        in_place_sort(v);
        auto sort_end = std::chrono::high_resolution_clock::now();
        
        auto sort_duration = std::chrono::duration_cast<std::chrono::microseconds>(sort_end - sort_start);
        double sort_time_ms = sort_duration.count() / 1000.0;
        total_sort_time += sort_time_ms;
        
        if (num_runs > 1) {
            write_intermediate_buffer_to_disk(v, i);
        } else {
            last_run = std::move(v);
        }
    }
    
    auto total_sort_end = std::chrono::high_resolution_clock::now();
    
    std::cout << "Sorted all runs\n";
    std::cout << "Total in-place sort time: " << std::fixed << std::setprecision(3) 
              << total_sort_time << " ms\n";

    if (num_runs == 1) {
        write_output_chunk(last_run.data(), config.file_size_bytes);
    } else {
        merge(last_run);
    }

}

template <uint32_t KeyLength, uint32_t ValueLength>
std::vector<KeyValuePair<KeyLength, ValueLength>> Sorter<KeyLength, ValueLength>::read_input_chunk(uint64_t chunk_id) {
    std::vector<KeyValuePair<KeyLength, ValueLength>> output_vector(config.run_size_bytes / sizeof(KeyValuePair<KeyLength, ValueLength>));
    auto ret = pread64(read_fd, output_vector.data(), config.run_size_bytes, chunk_id * config.run_size_bytes);
    return output_vector;
}

template <uint32_t KeyLength, uint32_t ValueLength>
void Sorter<KeyLength, ValueLength>::write_intermediate_buffer_to_disk(std::vector<KeyValuePair<KeyLength, ValueLength>> &vec, uint64_t chunk_idx) {
    uint64_t output_chunk_size = vec.size() * sizeof(KeyValuePair<KeyLength, ValueLength>);
    auto start = std::chrono::high_resolution_clock::now();

    auto ret = pwrite(intermediate_fd, vec.data(), output_chunk_size, 
        chunk_idx * output_chunk_size);
    auto end = std::chrono::high_resolution_clock::now();

    timing_info.intermediate_write += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;
}

template <uint32_t KeyLength, uint32_t ValueLength>
void Sorter<KeyLength, ValueLength>::merge(std::vector<KeyValuePair<KeyLength, ValueLength>> &last_run) {
    std::cout << "Merging runs...\n";
    uint64_t num_runs = config.num_runs();
    std::vector<SortedRun<KeyLength, ValueLength> *> current_runs;
    for (int i=0; i<num_runs; i++) {
        uint64_t start_offset = i * config.run_size_bytes;
        auto run = new SortedRun<KeyLength, ValueLength>(
            config.merge_read_chunk_size, intermediate_fd, start_offset, config.run_size_bytes
        );
        current_runs.push_back(run);
    }
    // Last run is read from memory, first `num_runs - 1` runs are read from the disk
    // current_runs.push_back(new SortedRun<KeyLength, ValueLength>(config.run_size_bytes, &last_run));
    void *write_buffer; 
    int ret = posix_memalign(&write_buffer, 4096, config.merge_write_chunk_size);
    uint64_t write_buffer_offset = 0;

    while (current_runs.size()) {
        KeyValuePair<KeyLength, ValueLength> smallest = current_runs[0]->get_next();
        uint32_t smallest_run_idx = 0;
        for (uint32_t i=1; i<current_runs.size(); i++) {
            auto start = std::chrono::high_resolution_clock::now();
            auto next = current_runs[i]->get_next();
            if (next < smallest) {
                smallest = next;
                smallest_run_idx = i;
            }
            auto end = std::chrono::high_resolution_clock::now();
            timing_info.merge_read += std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() / 1000.0;
        }
        current_runs[smallest_run_idx]->next();
        if (!current_runs[smallest_run_idx]->has_next()) {
            // delete(current_runs[smallest_run_idx]);
            current_runs.erase(current_runs.begin() + smallest_run_idx);
        }
        std::memcpy(reinterpret_cast<uint8_t*>(write_buffer) + write_buffer_offset, 
            reinterpret_cast<uint8_t*>(&smallest), sizeof(KeyValuePair<KeyLength, ValueLength>));

        write_buffer_offset += sizeof(KeyValuePair<KeyLength, ValueLength>);
        if (write_buffer_offset == config.merge_write_chunk_size) {
            write_output_chunk(write_buffer, config.merge_write_chunk_size);
            write_buffer_offset = 0ll;
        }
    }
    if (write_buffer_offset > 0) {
        auto start = std::chrono::high_resolution_clock::now();
        write_output_chunk(write_buffer, write_buffer_offset);
    }
}

template <uint32_t KeyLength, uint32_t ValueLength>
void Sorter<KeyLength, ValueLength>::write_output_chunk(void *buffer, uint64_t length) {
    auto start = std::chrono::high_resolution_clock::now();
    int res = pwrite64(write_fd, buffer, length, write_offset);
    auto end = std::chrono::high_resolution_clock::now();

    timing_info.output_write += std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() / 1000.0;
    write_offset += length;
}