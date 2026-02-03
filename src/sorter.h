#pragma once

#include <cassert>
#include <cmath>
#include <sched.h>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <iomanip>
#include <parallel/algorithm>
#include <algorithm>
#include <sys/mman.h>
#include <errno.h>

#include <pthread.h>

#include "perf_utils.h"
#include "key_value_pair.h"
#include "sorted_run.h"
#include "config.h"
#include "merge.h"
#include "read_values.h"
#define _REENTRANT
#include "ips4o.hpp"

#include <spdlog/spdlog.h>

typedef uint64_t IndexType;

constexpr uint32_t IndexLength = sizeof(IndexType);
// Linux cant read/write more than 2G in a single pread/pwrite call
constexpr uint64_t MAX_IO_CHUNK_SIZE = 1<<30;

struct TimingInfo {
    float intermediate_write;
    float output_write;
    float merge_read;
    float input_read;
    float sort_time;
    float value_separation;
    float value_write_back;
    float value_write_back_post_merge;
    float final_output_write;
    float create_intermediate_value_runs;
    float merge_in_memory;
};


void write_to_disk(void *buf, uint64_t file_offset, uint64_t bytes, int fd) {
    uint64_t offset = 0ll;
    while (bytes > 0) {
        uint64_t bytes_to_write = std::min(bytes, MAX_IO_CHUNK_SIZE);
        uint64_t ret = pwrite64(fd, (uint8_t*)buf + offset, bytes_to_write, file_offset + offset);
        assert(ret > 0);
        offset += ret;
        bytes -= ret;
    }
}

template <typename RecordType>
class Sorter {
    static constexpr uint32_t ELEM_SIZE = sizeof(RecordType);
    using KeyIndexPair = KeyValuePair<RecordType::KEY_LENGTH, IndexLength>;
    Config config;

    int read_fd; // File from which input records are read
    int write_fd; // File to which sorted records are written

    TimingInfo timing_info;
    // PerfInfo perf_info;

    std::vector<RecordType> read_input_chunk(uint64_t chunk_id);

    int write_intermediate_values(void *buf, uint64_t num_bytes, uint64_t chunk_idx);

    void write_output_chunk(void *buffer, uint64_t length);

    template <uint32_t ValueLength> 
    void sort_independently(std::vector<KeyValuePair<RecordType::KEY_LENGTH, ValueLength>> &vec);

    void merge(
        std::vector<std::vector<KeyIndexPair>> &sorted_runs, 
        uint64_t length, std::vector<MergeTask<KeyIndexPair>> *tasks);

    template <uint32_t ValueLength>
    void in_place_sort(std::vector<KeyValuePair<RecordType::KEY_LENGTH, ValueLength>> &vec);

    void merge(std::vector<SortedRun> &sorted_runs);

    template <uint32_t ValueLength>
    void in_place_std_sort(std::vector<KeyValuePair<RecordType::KEY_LENGTH, ValueLength>> &vec);

    void generate_key_index_pairs(std::vector<RecordType> &vec, 
        std::vector<KeyIndexPair> &key_index_pairs,
        void* value_buffer, bool separate_values);

    void write_back_values(std::vector<RecordType> &original,
        std::vector<KeyIndexPair> &key_index_pairs,
        void *value_buffer);

    void sort_single_run(std::vector<RecordType> &run);

    void generate_run_for_merge_sort(std::vector<RecordType> &run,  
        void *sorted_values, std::vector<KeyIndexPair> &key_index_pairs, int run_id);

    void write_back_values_post_merge(std::vector<int> &fds, std::vector<MergeTask<KeyIndexPair>> &tasks,
        std::vector<std::vector<KeyIndexPair>> &key_index_pairs);

public:
    Sorter(Config &&config) {
        this->config = std::move(config);
        timing_info = {};
        read_fd = -1;
        write_fd = -1;
    }

    void sort();

    void print_timing_stats() {
        spdlog::info("Read input: {} ms", timing_info.input_read);
        spdlog::info("Read merge phase: {} ms", timing_info.merge_read);
        spdlog::info("Write sorted runs to disk: {} ms", timing_info.intermediate_write);
        spdlog::info("Write output to disk: {} ms", timing_info.output_write);
        spdlog::info("Sorting time: {} ms", timing_info.sort_time);
        spdlog::info("Key-value separation: {} ms", timing_info.value_separation);
        spdlog::info("Value write back (for one-pass sort): {} ms", timing_info.value_write_back);
        spdlog::info("Creation of intermediate value runs: {} ms", timing_info.create_intermediate_value_runs);
        spdlog::info("In-memory merge: {} ms", timing_info.merge_in_memory);
        spdlog::info("Write back values (after merge sort): {} ms", timing_info.value_write_back_post_merge);
        spdlog::info("Write final output to disk (after merge sort): {} ms", timing_info.final_output_write);
    }
};

template <typename RecordType>
void Sorter<RecordType>::generate_key_index_pairs(std::vector<RecordType> &vec, 
        std::vector<KeyIndexPair> &key_index_pairs, void* value_buffer, bool separate_values) {
    spdlog::debug("Start separating values from keys");
    auto start = std::chrono::high_resolution_clock::now();
    #pragma omp parallel for num_threads(config.num_threads)
    for (uint64_t i=0; i<(uint64_t)vec.size(); i++) {
        key_index_pairs[i].key = __builtin_bswap64(vec[i].key);
        key_index_pairs[i].set_value(&i);
        uint64_t offset = i * RecordType::VALUE_LENGTH;

        if (separate_values) {
            std::memcpy((uint8_t*)value_buffer + offset, &vec[i].value, RecordType::VALUE_LENGTH);
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    timing_info.value_separation += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;

    spdlog::debug("Done separating values from keys");
}

template <typename RecordType>
void Sorter<RecordType>::write_back_values(std::vector<RecordType> &original,
        std::vector<KeyIndexPair> &key_index_pairs,
        void *value_buffer) {
    int miss_fd = init_perf_counter(PERF_COUNT_HW_CACHE_MISSES);
    int ref_fd = init_perf_counter(PERF_COUNT_HW_CACHE_REFERENCES);

    spdlog::debug("Start writing back values");
    auto start = std::chrono::high_resolution_clock::now();
    // TODO(): Maybe unroll loop??
    #pragma omp parallel for num_threads(config.num_threads)
    for (uint64_t i=0; i<key_index_pairs.size(); i++) {
        original[i].set_key(&key_index_pairs[i].key);
        void *value_ptr = (uint8_t*)value_buffer + key_index_pairs[i].value * RecordType::VALUE_LENGTH;
        original[i].set_value(value_ptr);
    }
    auto end = std::chrono::high_resolution_clock::now();
    timing_info.value_write_back += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;
    spdlog::debug("Done writing back values");

    uint64_t lld_misses = read_perf_counter(miss_fd);
    uint64_t lld_hits = read_perf_counter(ref_fd);
    float lld_miss_rate = lld_misses * 100.0f / (lld_misses + lld_hits);
    spdlog::info("lld miss rate: {}", lld_miss_rate);
}

template <typename RecordType>
template <uint32_t ValueLength>
void Sorter<RecordType>::in_place_std_sort(std::vector<KeyValuePair<RecordType::KEY_LENGTH, ValueLength>> &vec) {
    spdlog::debug("Start in-place sort");

    auto start = std::chrono::high_resolution_clock::now();
    if (config.num_threads > 1) {
        __gnu_parallel::sort(vec.begin(), vec.end(), std::less<KeyValuePair<RecordType::KEY_LENGTH, ValueLength>>{});
    } else {
        std::sort(vec.begin(), vec.end(), std::less<KeyValuePair<RecordType::KEY_LENGTH, ValueLength>>{});
    }
    auto end = std::chrono::high_resolution_clock::now();

    spdlog::debug("Done in-place sort");
    timing_info.sort_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;
}

template <typename RecordType>
template <uint32_t ValueLength>
void Sorter<RecordType>::in_place_sort(std::vector<KeyValuePair<RecordType::KEY_LENGTH, ValueLength>> &vec) {
    spdlog::debug("Start in-place sort");
    int miss_fd = init_perf_counter(PERF_COUNT_HW_CACHE_MISSES);
    int ref_fd = init_perf_counter(PERF_COUNT_HW_CACHE_REFERENCES);
    int cycles_fd = init_perf_counter(PERF_COUNT_HW_CPU_CYCLES);
    int instructions_fd = init_perf_counter(PERF_COUNT_HW_INSTRUCTIONS);
    

    // int tlb_miss_fd = init_perf_counter_cache((PERF_COUNT_HW_CACHE_DTLB << 0) 
    //     | (PERF_COUNT_HW_CACHE_OP_READ << 8) 
    //     | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16));
    // int tlb_ref_fd = init_perf_counter_cache((PERF_COUNT_HW_CACHE_DTLB << 0) 
    //     | (PERF_COUNT_HW_CACHE_OP_READ << 8) 
    //     | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16));
    int branch_miss_fd = init_perf_counter(PERF_COUNT_HW_BRANCH_MISSES);
    int branch_retired_fd = init_perf_counter(PERF_COUNT_HW_BRANCH_INSTRUCTIONS);

    auto start = std::chrono::high_resolution_clock::now();
    ips4o::parallel::sort(vec.begin(), vec.end(), std::less<KeyValuePair<RecordType::KEY_LENGTH, ValueLength>>{}, config.num_threads);
    auto end = std::chrono::high_resolution_clock::now();
    spdlog::debug("Done in-place sort");

    int cache_misses = read_perf_counter(miss_fd);
    int refs = read_perf_counter(ref_fd);
    float miss_ratio = 100.0f * cache_misses / ((float)cache_misses + (float)refs);
    spdlog::debug("Miss ratio: {}", miss_ratio);

    // uint64_t tlb_misses = read_perf_counter(tlb_miss_fd);
    // uint64_t tlb_refs = read_perf_counter(tlb_ref_fd);
    uint64_t branch_misses = read_perf_counter(branch_miss_fd);
    uint64_t branches_retired = read_perf_counter(branch_retired_fd);
    uint64_t cycles = read_perf_counter(cycles_fd);
    uint64_t instructions = read_perf_counter(instructions_fd);

    float branch_miss_ratio = (branch_misses * 100.0f) / branches_retired;

    float cpi = (cycles * 1.0f) / instructions;
    spdlog::debug("Branch miss %: {}", branch_miss_ratio);
    spdlog::debug("CPI: {}", cpi);
    
    // float tlb_miss_rate = tlb_misses * 100.0f / (tlb_misses + tlb_refs);
    // spdlog::info("TLB miss rate: {}", tlb_miss_rate);

    timing_info.sort_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;
}

template <typename RecordType>
template <uint32_t ValueLength>
void Sorter<RecordType>::sort_independently(std::vector<KeyValuePair<RecordType::KEY_LENGTH, ValueLength>> &vec) {
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

template<typename RecordType>
void Sorter<RecordType>::sort_single_run(std::vector<RecordType> &run) {
    if (config.separate_values) {
        std::vector<KeyIndexPair> key_index_pairs(run.size());
        void *value_buffer;
        int ret = posix_memalign(&value_buffer, 64, RecordType::VALUE_LENGTH * run.size());
        assert(ret == 0);
        memset(value_buffer, 0, RecordType::VALUE_LENGTH * run.size());

        generate_key_index_pairs(run, key_index_pairs, value_buffer, true);
        in_place_sort<IndexLength>(key_index_pairs);
        write_back_values(run, key_index_pairs, value_buffer);

        spdlog::info("is sorted: {}", std::is_sorted(run.begin(), run.end()));
        free(value_buffer);
    } else {
        in_place_sort<RecordType::VALUE_LENGTH>(run);
    }
}

template<typename RecordType>
void Sorter<RecordType>::generate_run_for_merge_sort(
        std::vector<RecordType> &run, 
        void *sorted_values, std::vector<KeyIndexPair> &key_index_pairs, int run_id) {
    assert(config.separate_values);
    assert((run.size() * sizeof(RecordType)) % 64 == 0);
    key_index_pairs.resize(run.size());
    memset(key_index_pairs.data(), 0, run.size() * sizeof(KeyIndexPair));

    generate_key_index_pairs(run, key_index_pairs, nullptr, false);
    in_place_sort<IndexLength>(key_index_pairs);

    auto start = std::chrono::high_resolution_clock::now();
    #pragma omp parallel for num_threads(config.num_threads)
    for (int i=0; i<run.size(); i++) {
        void *value_ptr = (uint8_t*)run.data() + key_index_pairs[i].value * sizeof(RecordType);
        std::memcpy((uint8_t*) sorted_values + i * RecordType::VALUE_LENGTH, value_ptr, RecordType::VALUE_LENGTH);
        key_index_pairs[i].value = (uint64_t)run_id;
    }
    auto end = std::chrono::high_resolution_clock::now();
    timing_info.create_intermediate_value_runs += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;
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
        sort_single_run(v);
        write_output_chunk(v.data(), config.file_size_bytes);
        return;
    }

    uint64_t memory_size_bytes = config.run_size_bytes;
    uint64_t merge_chunk_size_bytes = (memory_size_bytes / (num_runs + 1));
    // align this to 4096 bytes for direct IO
    merge_chunk_size_bytes = (merge_chunk_size_bytes / Config::BLOCK_SIZE_ALIGN) * Config::BLOCK_SIZE_ALIGN;
    config.merge_read_chunk_size = merge_chunk_size_bytes;
    config.merge_write_chunk_size = merge_chunk_size_bytes;

    std::vector<SortedRun> sorted_runs;
    std::vector<std::vector<KeyIndexPair>> key_index_pairs(num_runs);
    std::vector<int> fds;

    for (int i=0; i<num_runs; i++) {
        auto v = read_input_chunk(i);
        void *sorted_values;
        int ret = posix_memalign(&sorted_values, 4096, RecordType::VALUE_LENGTH * v.size());
        assert(ret == 0);
        memset(sorted_values, 0, RecordType::VALUE_LENGTH * v.size());
        generate_run_for_merge_sort(v, sorted_values, key_index_pairs[i], i);
        fds.push_back(
            write_intermediate_values(sorted_values, v.size() * RecordType::VALUE_LENGTH, i)
        );
        free(sorted_values);
    }
    std::vector<MergeTask<KeyIndexPair>> tasks;
    merge(key_index_pairs, config.run_size_bytes / sizeof(RecordType), &tasks);
    write_back_values_post_merge(fds, tasks, key_index_pairs);
}

template <typename RecordType>
std::vector<RecordType> Sorter<RecordType>::read_input_chunk(uint64_t chunk_id) {
    spdlog::debug("Start reading input");
    std::vector<RecordType> output_vector(config.run_size_bytes / ELEM_SIZE);
    // Avoid minor page faults during read
    memset((void*)output_vector.data(), 0, config.run_size_bytes);
    
    auto start = std::chrono::high_resolution_clock::now();
    uint64_t length = config.run_size_bytes;
    uint64_t file_offset = chunk_id * config.run_size_bytes;
    uint64_t io_offset = 0;

    while (length > 0) {
        uint64_t bytes_to_read = std::min(MAX_IO_CHUNK_SIZE, length);
        auto ret = pread64(read_fd, (uint8_t*)output_vector.data() + io_offset, bytes_to_read, file_offset + io_offset);
        assert(ret == bytes_to_read);
        length -= bytes_to_read;
        io_offset += bytes_to_read;
    }
    auto end = std::chrono::high_resolution_clock::now();
    
    timing_info.input_read += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;
    spdlog::debug("Done reading input");
    return output_vector;
}

template <typename RecordType>
int Sorter<RecordType>::write_intermediate_values(void *buf, uint64_t num_bytes, uint64_t chunk_idx) {
    std::string file_name = config.intermediate_file_prefix + "-chunk-" + std::to_string(chunk_idx);
    int fd = open(file_name.c_str(), O_CREAT | O_RDWR | O_TRUNC | O_DIRECT, 0644);
    assert(fd != -1);
    spdlog::debug("Writing {} bytes to file: {}", num_bytes, file_name);

    uint64_t length = num_bytes;
    auto start = std::chrono::high_resolution_clock::now();
    write_to_disk(buf, 0ll, num_bytes, fd);
    auto end = std::chrono::high_resolution_clock::now();

    timing_info.intermediate_write += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;
    return fd;
}

template <typename RecordType>
void Sorter<RecordType>::merge(
        std::vector<std::vector<KeyIndexPair>> &sorted_runs, 
        uint64_t run_length, std::vector<MergeTask<KeyIndexPair>> *tasks) {
    using ReaderType = SortedRunReader<RecordType>;

    spdlog::info("Starting merge step...\n");
    auto start = std::chrono::high_resolution_clock::now();

    *tasks = create_tasks<KeyIndexPair>(sorted_runs, run_length, config.num_threads);
    spdlog::info("Created {} tasks", tasks->size());

    std::vector<std::thread> threads;
    bool result_sorted[tasks->size()];
    for (int i=0; i<tasks->size(); i++) {
        threads.emplace_back(std::thread(run_merge_avx_512<KeyIndexPair>, &tasks->at(i), result_sorted + i));
    }
    bool all_true = true;
    for (int i=0; i<tasks->size(); i++) {
        threads[i].join();
        all_true &= result_sorted[i];
        spdlog::debug("Task {} result sorted: {}", i, result_sorted[i]);
    }
    spdlog::info("Finished merging key-stream id pairs. All sorted: {}", all_true);

    auto end = std::chrono::high_resolution_clock::now();
    timing_info.merge_in_memory = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;
}

template <typename RecordType>
void Sorter<RecordType>::write_output_chunk(void *buffer, uint64_t length) {
    spdlog::debug("Start writing final output");

    auto start = std::chrono::high_resolution_clock::now();
    write_to_disk(buffer, 0ll, length, write_fd);
    auto end = std::chrono::high_resolution_clock::now();

    timing_info.output_write += std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() / 1000.0;

    spdlog::debug("Done writing final output");
}

template <typename RecordType>
void Sorter<RecordType>::write_back_values_post_merge(std::vector<int> &fds, 
        std::vector<MergeTask<KeyIndexPair>> &tasks,
        std::vector<std::vector<KeyIndexPair>> &key_index_pairs) {
    spdlog::debug("Start writing back values post merge");
    
    uint32_t num_streams = tasks[0].start_ptrs.size();
    std::vector<void*> output_ptrs;

    for (int i=0; i<tasks.size(); i++) {
        void *sorted_output;
        uint64_t output_size = sizeof(RecordType) * tasks[i].total_records_sorted;
        uint64_t alloc_sz = (output_size + BLOCK_ALIGN - 1) / BLOCK_ALIGN * BLOCK_ALIGN;
        int ret = posix_memalign(&sorted_output, BLOCK_ALIGN, alloc_sz);
        assert(ret == 0);
        memset(sorted_output, 0, alloc_sz);
        output_ptrs.push_back(sorted_output);
    }
    auto start = std::chrono::high_resolution_clock::now();

    #pragma omp parallel for num_threads(config.num_threads)
    for (int i=0; i<tasks.size(); i++) {
        void *sorted_output = output_ptrs[i];
        std::vector<ValueReader> value_readers;
        for (int j=0; j<num_streams; j++) {
            uint64_t record_offset = uint64_t(tasks[i].start_ptrs[j] - key_index_pairs[j].data());
            ValueReader value_reader {fds[j], RecordType::VALUE_LENGTH, record_offset};
            value_readers.push_back(value_reader);
        }

        RecordType *output_ptr = (RecordType*) sorted_output;
        KeyIndexPair *ptr = (KeyIndexPair*) tasks[i].output;
        
        for (int j=0; j<tasks[i].total_records_sorted; j++) {
            output_ptr->key = __builtin_bswap64(ptr->key);
            uint32_t stream_id = ptr->value;
            void *value = value_readers[stream_id].read_next();
            std::memcpy(&output_ptr->value, value, RecordType::VALUE_LENGTH);
            output_ptr++;
            ptr++;
        }
    }
    auto end = std::chrono::high_resolution_clock::now();

    timing_info.value_write_back_post_merge += std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() / 1000.0;

    // Write each task's sorted buffer to its own output file
    auto write_start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < tasks.size(); i++) {
        uint64_t output_size = sizeof(RecordType) * tasks[i].total_records_sorted;
        std::string path = config.output_file + "-task-" + std::to_string(i);
        int fd = open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC | O_DIRECT, 0644);
        assert(fd != -1);
        write_to_disk(output_ptrs[i], 0, output_size, fd);
        close(fd);
    }
    auto write_end = std::chrono::high_resolution_clock::now();
    timing_info.final_output_write += std::chrono::duration_cast<std::chrono::microseconds>(write_end - write_start).count() / 1000.0f;

    for (size_t i = 0; i < output_ptrs.size(); i++) {
        free(output_ptrs[i]);
    }

    spdlog::debug("Done writing back values post merge");
}
