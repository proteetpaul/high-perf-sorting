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
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>

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

struct PerfInfo {
    std::vector<int> cache_misses_fd;
    std::vector<int> cache_refs_fd;
    // std::vector<int> l2_misses_fd;
};

struct read_format {
    uint64_t value;
    uint64_t id;
};

static int perf_event_open(struct perf_event_attr *hw_event, pid_t pid, int cpu, int group_fd, unsigned long flags){
    int fd;
    fd = syscall(SYS_perf_event_open, hw_event, pid, cpu, group_fd, flags);
    if (fd == -1) {
        fprintf(stderr, "Error creating event\n");
        exit(EXIT_FAILURE);
    }
  
    return fd;
}

void configure_event(struct perf_event_attr *pe, uint64_t config){
    memset(pe, 0, sizeof(struct perf_event_attr));
    pe->type = PERF_TYPE_HARDWARE;
    pe->size = sizeof(struct perf_event_attr);
    pe->config = config;
    pe->read_format = PERF_FORMAT_ID;
    pe->disabled = 1;
    pe->exclude_kernel = 1;
    pe->exclude_hv = 1;
}

template <uint32_t KeyLength, uint32_t ValueLength>
class Sorter {
    static constexpr uint32_t ELEM_SIZE = sizeof(KeyValuePair<KeyLength, ValueLength>);
    Config config;

    int read_fd; // File from which input records are read
    int write_fd; // File to which sorted records are written

    TimingInfo timing_info;
    PerfInfo perf_info;
    uint64_t output_file_offset;

    std::vector<KeyValuePair<KeyLength, ValueLength>> read_input_chunk(uint64_t chunk_id);

    SortedRun create_disk_run(std::vector<KeyValuePair<KeyLength, ValueLength>> &vec, uint64_t chunk_idx);

    void write_output_chunk(void *buffer, uint64_t length);

    void in_place_sort(std::vector<KeyValuePair<KeyLength, ValueLength>> &vec);

    void merge(std::vector<SortedRun> &sorted_runs);

    void init_perf_counters() {
        struct perf_event_attr pe[2];
        configure_event(&pe[0], PERF_COUNT_HW_CACHE_MISSES);
        configure_event(&pe[1], PERF_COUNT_HW_CACHE_REFERENCES);
        
        for (int i=0; i<2; i++) {
            perf_event_attr event_attr = pe[i];
            int fd = perf_event_open(&event_attr, 0, -1, -1, 0);
            ioctl(fd, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
            ioctl(fd, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);

            if (i == 0) {
                perf_info.cache_misses_fd.push_back(fd);
            } else {
                perf_info.cache_refs_fd.push_back(fd);
            }
        }
    }

    void read_perf_counters() {
        ioctl(perf_info.cache_misses_fd[0], PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
        ioctl(perf_info.cache_refs_fd[0], PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);

        read_format cache_misses, cache_refs;
        int ret = read(perf_info.cache_misses_fd[0], &cache_misses, sizeof(struct read_format));
        ret = read(perf_info.cache_refs_fd[0], &cache_refs, sizeof(struct read_format));
        printf("Cache refs: %ld\n", cache_refs.value);
        printf("Cache misses: %ld\n", cache_misses.value);
        float miss_ratio = 100.0f * cache_misses.value / ((float)cache_misses.value + (float)cache_refs.value);
        printf("Cache miss ratio: %f\n", miss_ratio);
    }

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
        init_perf_counters();
        in_place_sort(v);
        read_perf_counters();
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

    read_perf_counters();
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

template <uint32_t KeyLength, uint32_t ValueLength>
void Sorter<KeyLength, ValueLength>::write_output_chunk(void *buffer, uint64_t length) {
    auto start = std::chrono::high_resolution_clock::now();
    int ret = pwrite64(write_fd, buffer, length, output_file_offset);
    assert(ret == length);
    auto end = std::chrono::high_resolution_clock::now();

    timing_info.output_write += std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() / 1000.0;
    output_file_offset += length;
}