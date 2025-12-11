#pragma once
// #include "commons.h"
#include "config.h"
#include "partition.h"
#include "Origami/merge_tree.h"

#include <spdlog/spdlog.h>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sys/mman.h>

constexpr uint64_t CACHE_LINE_SIZE = 64;
constexpr uint64_t MERGE_BUF_SIZE = 1 << 20;
constexpr uint64_t L1_CACHE_SIZE = 1 << 20;
constexpr uint64_t L2_CACHE_SIZE = 1 << 20;

template<typename RecordType>
struct MergeTask {
    std::vector<RecordType*> start_ptrs;
    std::vector<uint64_t> num_records;
    void *output;
    // Could be different from sum of num_records if alignment is taken into account
    uint64_t total_records_sorted;
};

template<typename RecordType>
std::vector<MergeTask<RecordType>> create_tasks(std::vector<std::vector<RecordType>> &sorted_runs, uint64_t run_length, int num_threads) {
    std::vector<RecordType*> start_ptrs(sorted_runs.size());
    std::vector<uint64_t> run_lengths(sorted_runs.size());
    for (int i=0; i<sorted_runs.size(); i++) {
        start_ptrs[i] = sorted_runs[i].data();
        run_lengths[i] = run_length;
    }
    int depth = std::log2(num_threads);
    spdlog::info("Generating partitions");
    auto partitions = generate_partitions(start_ptrs, run_lengths, depth);
    spdlog::info("Generated partitions");
    std::vector<MergeTask<RecordType>> tasks(partitions.first.size());
    for (int i=0; i<partitions.first.size(); i++) {
        tasks[i].start_ptrs = partitions.first[i];
        tasks[i].num_records = partitions.second[i];
    }
    return tasks;
}

template<typename RecordType>
void run_merge_avx_512(MergeTask<RecordType> *task) {
    using Regtype = avx512;
    using ItemType = KeyValue<i64, i64>;
    int num_streams = task->start_ptrs.size();
    std::vector<ItemType*> end_ptrs;
    std::vector<ItemType*> start_ptrs;
    uint64_t total_records;

    for (int i=0; i<num_streams; i++) {
        uint64_t start_align = uint64_t(task->start_ptrs[i]) & (CACHE_LINE_SIZE-1);
        // uint8_t *start_ptr = (uint8_t*) task->start_ptrs[i] - start_align;
        task->start_ptrs[i] -= start_align/sizeof(RecordType);
        uint8_t *end_ptr = (uint8_t*) task->start_ptrs[i] + task->num_records[i] * sizeof(RecordType);
        uint64_t end_align = CACHE_LINE_SIZE - (uint64_t)end_ptr & (CACHE_LINE_SIZE - 1);

        end_ptrs.push_back((ItemType*)end_ptr);
        start_ptrs.push_back((ItemType*)task->start_ptrs[i]);
        task->total_records_sorted += task->num_records[i] + (start_align + end_align) / sizeof(RecordType);
    }

    task->output = mmap(NULL, task->total_records_sorted * sizeof(RecordType), PROT_READ | PROT_WRITE, MAP_ANONYMOUS, -1, 0);
    assert(task->output != nullptr);

    uint32_t tree_height = std::log2(num_streams);
    std::unique_ptr<origami_merge_tree::MergeTree<Regtype, ItemType>> merge_tree;
    if (tree_height % 2 == 0) {
        merge_tree = std::make_unique<origami_merge_tree::MergeTreeEven<Regtype, ItemType>>();
    } else {
        merge_tree = std::make_unique<origami_merge_tree::MergeTreeOdd<Regtype, ItemType>>();
    }
    void *intermediate_buf = mmap(NULL, MERGE_BUF_SIZE, PROT_READ | PROT_WRITE, MAP_ANONYMOUS, -1, 0);
    assert(intermediate_buf != NULL);
    merge_tree->merge_init(num_streams, (ItemType*)intermediate_buf, L1_CACHE_SIZE/sizeof(RecordType), L2_CACHE_SIZE/sizeof(RecordType));
    merge_tree->merge((ItemType**)start_ptrs.data(), (ItemType**)end_ptrs.data(), (ItemType*)task->output, total_records, L1_CACHE_SIZE/sizeof(ItemType), 
        L2_CACHE_SIZE/sizeof(ItemType), nullptr, num_streams);
}