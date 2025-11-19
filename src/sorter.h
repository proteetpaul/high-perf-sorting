#pragma once

#include <cassert>
#include <sched.h>
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

#include <pthread.h>

#include "perf_utils.h"
#include "key_value_pair.h"
#include "sorted_run.h"
#include "config.h"
#include "merge_tree.h"
#define _REENTRANT
#include "ips4o.hpp"

#include <spdlog/spdlog.h>

typedef uint32_t IndexType;

constexpr uint32_t IndexLength = sizeof(IndexType);
// Linux cant read/write more than 2G in a single pread/pwrite call
constexpr uint64_t MAX_IO_CHUNK_SIZE = 1<<30;

struct TimingInfo {
    float intermediate_write;
    float output_write;
    float merge_read;
    float input_read;
    float sort_time;
};


template <typename RecordType>
class Sorter {
    static constexpr uint32_t ELEM_SIZE = sizeof(RecordType);
    Config config;

    int read_fd; // File from which input records are read
    int write_fd; // File to which sorted records are written

    TimingInfo timing_info;
    // PerfInfo perf_info;
    uint64_t output_file_offset;

    std::vector<RecordType> read_input_chunk(uint64_t chunk_id);

    SortedRun create_disk_run(std::vector<RecordType> &vec, uint64_t chunk_idx);

    void write_output_chunk(void *buffer, uint64_t length);

    template <uint32_t ValueLength> 
    void in_place_sort(std::vector<KeyValuePair<RecordType::KEY_LENGTH, ValueLength>> &vec);

    void merge(std::vector<SortedRun> &sorted_runs);

    template <uint32_t ValueLength>
    void in_place_sort2(std::vector<KeyValuePair<RecordType::KEY_LENGTH, ValueLength>> &vec);

    void separate_values(std::vector<RecordType> &vec, 
        std::vector<KeyValuePair<RecordType::KEY_LENGTH, IndexLength>> &key_index_pairs,
        void* value_buffer);

    void write_back_values(std::vector<RecordType> &original,
        std::vector<KeyValuePair<RecordType::KEY_LENGTH, IndexLength>> &key_index_pairs,
        void *value_buffer);

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

template <typename RecordType>
void Sorter<RecordType>::separate_values(std::vector<RecordType> &vec, 
        std::vector<KeyValuePair<RecordType::KEY_LENGTH, IndexLength>> &key_index_pairs, void* value_buffer) {
    uint64_t value_buffer_offset = 0ll;
    for (uint32_t i=0; i<(uint32_t)vec.size(); i++) {
        key_index_pairs[i].set_key(&vec[i].key);
        key_index_pairs[i].set_value(&i);
        
        std::memcpy((uint8_t*)value_buffer + value_buffer_offset, vec[i].value, RecordType::VALUE_LENGTH);
        value_buffer_offset += RecordType::VALUE_LENGTH;
    }
}

template <typename RecordType>
void Sorter<RecordType>::write_back_values(std::vector<RecordType> &original,
        std::vector<KeyValuePair<RecordType::KEY_LENGTH, IndexLength>> &key_index_pairs,
        void *value_buffer) {
    for (uint64_t i=0; i<key_index_pairs.size(); i++) {
        original[i].set_key(&key_index_pairs[i].key);
        void *value_ptr = (uint8_t*)value_buffer + key_index_pairs[i].value * RecordType::VALUE_LENGTH;
        original[i].set_value(value_ptr);
    }
}

template <typename RecordType>
template <uint32_t ValueLength>
void Sorter<RecordType>::in_place_sort2(std::vector<KeyValuePair<RecordType::KEY_LENGTH, ValueLength>> &vec) {
    int miss_fd = init_perf_counter(PERF_COUNT_HW_CACHE_MISSES);
    int ref_fd = init_perf_counter(PERF_COUNT_HW_CACHE_REFERENCES);

    auto start = std::chrono::high_resolution_clock::now();
    ips4o::parallel::sort(vec.begin(), vec.end(), std::less<KeyValuePair<RecordType::KEY_LENGTH, ValueLength>>{}, config.num_threads);
    auto end = std::chrono::high_resolution_clock::now();

    int cache_misses = read_perf_counter(miss_fd);
    int refs = read_perf_counter(ref_fd);
    float miss_ratio = 100.0f * cache_misses / ((float)cache_misses + (float)refs);
    spdlog::info("Miss ratio: {}", miss_ratio);

    timing_info.sort_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;
}

template <typename RecordType>
template <uint32_t ValueLength>
void Sorter<RecordType>::in_place_sort(std::vector<KeyValuePair<RecordType::KEY_LENGTH, ValueLength>> &vec) {
    std::vector<float> cache_misses_vec(config.num_threads);
    pthread_barrier_t barrier;
    int s = pthread_barrier_init(&barrier, NULL, config.num_threads);
    std::chrono::high_resolution_clock::time_point actual_start_time, actual_end_time;

    auto sort_task = [this, &vec, &cache_misses_vec, &barrier, &actual_start_time, &actual_end_time](uint64_t start_offset, uint64_t end_offset, int id) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(id + 1, &cpuset);
        pthread_t current_thread = pthread_self();
        pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);

        unsigned int cpu, node;
        {
            getcpu(&cpu, &node);
            spdlog::info("Thread {} started running on {} ", id, cpu);
        }
        int miss_fd = init_perf_counter(PERF_COUNT_HW_CACHE_MISSES);
        int ref_fd = init_perf_counter(PERF_COUNT_HW_CACHE_REFERENCES);

        pthread_barrier_wait(&barrier);
        
        if (id == 0) {
            actual_start_time = std::chrono::high_resolution_clock::now();
        }

        auto start = std::chrono::high_resolution_clock::now();
        ips4o::parallel::sort(vec.begin() + start_offset, vec.begin() + end_offset, 
            std::less<KeyValuePair<RecordType::KEY_LENGTH, ValueLength>>{}, 1);
        auto end = std::chrono::high_resolution_clock::now();

        auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;
        pthread_barrier_wait(&barrier);
        if (id == 0) {
            actual_end_time = std::chrono::high_resolution_clock::now();
        }
        {
            getcpu(&cpu, &node);
            spdlog::info("Thread {} ended running on {} ", id, cpu);
        }
        int cache_misses = read_perf_counter(miss_fd);
        int refs = read_perf_counter(ref_fd);
        float miss_ratio = 100.0f * cache_misses / ((float)cache_misses + (float)refs);
        cache_misses_vec[id] = miss_ratio;
        spdlog::info("Task {} took: {} ms", id, time_elapsed);
    };
    uint64_t stride = (vec.size()) / config.num_threads;
    std::vector<std::thread> tasks;
    tasks.reserve(config.num_threads);

    // auto start = std::chrono::high_resolution_clock::now();
    // ips4o::parallel::sort(vec.begin(), vec.end(), std::less<RecordType>{}, config.num_threads);
    uint64_t start_offset = 0ll;
    uint64_t end_offset = stride;
    for (int i=0; i<config.num_threads; i++) {
        tasks.emplace_back(sort_task, start_offset, end_offset, i);
        start_offset += stride;
        end_offset += stride;
    }
    for (int i=0; i<config.num_threads; i++) {
        tasks[i].join();
    }
    auto end = std::chrono::high_resolution_clock::now();

    float avg_cache_miss = 0.0f;
    for (float f: cache_misses_vec) {
        avg_cache_miss += f;
    }
    spdlog::info("Average cache misses: {} %", avg_cache_miss/config.num_threads);

    timing_info.sort_time += std::chrono::duration_cast<std::chrono::microseconds>(actual_end_time - actual_start_time).count() / 1000.0f;
}

template <typename RecordType>
void Sorter<RecordType>::sort() {
    // Ensure that elements don't span across on-disk blocks
    assert(Config::BLOCK_SIZE_ALIGN % ELEM_SIZE == 0);

    read_fd = open(config.input_file.c_str(), O_RDONLY | O_DIRECT);
    write_fd = open(config.output_file.c_str(), O_CREAT | O_RDWR | O_TRUNC | O_DIRECT, 0644);

    uint64_t num_runs = config.num_runs();
    if (num_runs == 1) {
        auto v = read_input_chunk(0);
        if (config.separate_values) {
            spdlog::info("Separating values from keys");
            std::vector<KeyValuePair<RecordType::KEY_LENGTH, IndexLength>> key_index_pairs(v.size());
            void *value_buffer;
            int ret = posix_memalign(&value_buffer, 64, RecordType::VALUE_LENGTH * v.size());
            assert(ret == 0);

            separate_values(v, key_index_pairs, value_buffer);
            in_place_sort2<IndexLength>(key_index_pairs);
            write_back_values(v, key_index_pairs, value_buffer);
            free(value_buffer);
        } else {
            in_place_sort2<RecordType::VALUE_LENGTH>(v);
        }
        write_output_chunk(v.data(), config.file_size_bytes);
        return;
    }

    assert(config.separate_values == false);

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
        in_place_sort2<RecordType::VALUE_LENGTH>(v);
        sorted_runs.push_back(create_disk_run(v, i));    
    }
    merge(sorted_runs);
}

template <typename RecordType>
std::vector<RecordType> Sorter<RecordType>::read_input_chunk(uint64_t chunk_id) {
    std::vector<RecordType> output_vector(config.run_size_bytes / ELEM_SIZE);
    
    auto start = std::chrono::high_resolution_clock::now();
    uint64_t length = config.run_size_bytes;
    uint64_t offset = chunk_id * config.run_size_bytes;
    while (length > 0) {
        uint64_t bytes_to_read = std::min(MAX_IO_CHUNK_SIZE, length);
        auto ret = pread64(read_fd, output_vector.data(), bytes_to_read, offset);
        assert(ret == bytes_to_read);
        length -= bytes_to_read;
        offset += length;
    }
    auto end = std::chrono::high_resolution_clock::now();
    
    timing_info.input_read += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;
    return output_vector;
}

template <typename RecordType>
SortedRun Sorter<RecordType>::create_disk_run(std::vector<RecordType> &vec, uint64_t chunk_idx) {
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

template <typename RecordType>
void Sorter<RecordType>::merge(std::vector<SortedRun> &sorted_runs) {
    using ReaderType = SortedRunReader<RecordType>;

    printf("Starting merge step...\n");
    auto start = std::chrono::high_resolution_clock::now();

    uint64_t num_runs = config.num_runs();
    std::vector<std::shared_ptr<ReaderType>> readers;
    for (auto& run: sorted_runs) {
        readers.push_back(std::make_shared<ReaderType>(config.merge_read_chunk_size, run));
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
            reinterpret_cast<uint8_t*>(&top_record), ELEM_SIZE);
        write_buffer_offset += ELEM_SIZE;
        if (write_buffer_offset == config.merge_write_chunk_size) {
            write_output_chunk(write_buffer, config.merge_write_chunk_size);
            write_buffer_offset = 0ll;
        }
    }

    if (write_buffer_offset > 0) {
        write_output_chunk(write_buffer, write_buffer_offset);
    }

    for (auto reader: readers) {
        timing_info.merge_read += reader->get_file_read_time()/1000.0f;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto merge_duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "Total merge time: " << std::fixed << std::setprecision(3) 
            << merge_duration.count() / 1000.0 << " ms\n";
}

template <typename RecordType>
void Sorter<RecordType>::write_output_chunk(void *buffer, uint64_t length) {
    auto start = std::chrono::high_resolution_clock::now();
    while (length > 0) {
        uint64_t bytes_to_write = std::min(length, MAX_IO_CHUNK_SIZE);
        uint64_t ret = pwrite64(write_fd, buffer, bytes_to_write, output_file_offset);
        assert(ret == bytes_to_write);
        output_file_offset += bytes_to_write;
        length -= bytes_to_write;
    }
    auto end = std::chrono::high_resolution_clock::now();

    timing_info.output_write += std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() / 1000.0;
    output_file_offset += length;
}