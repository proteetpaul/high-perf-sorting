#pragma once
#include "Origami/commons.h"
#include "Origami/merge_tree.h"
#include "config.h"
#include "partition.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <spdlog/spdlog.h>
#include <sys/mman.h>

constexpr uint64_t CACHE_LINE_SIZE = 64;

constexpr uint64_t L1_CACHE_SIZE = 1 << 12;
constexpr uint64_t L2_CACHE_SIZE = 1 << 12;

constexpr uint64_t MERGE_BUF_SIZE = L1_CACHE_SIZE + L2_CACHE_SIZE;

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
    spdlog::debug("Generating partitions");
    auto partitions = generate_partitions(start_ptrs, run_lengths, depth);
    spdlog::debug("Generated partitions");
    std::vector<MergeTask<RecordType>> tasks(partitions.first.size());
    for (int i=0; i<partitions.first.size(); i++) {
        tasks[i].start_ptrs = partitions.first[i];
        tasks[i].num_records = partitions.second[i];
    }
    return tasks;
}

template<typename RecordType>
void run_merge_avx_512(MergeTask<RecordType> *task, bool *result_sorted) {
    using Regtype = avx512;
    using ItemType = RecordType;
    static_assert(std::is_same_v<decltype(ItemType::key), int64_t>);
    static_assert(std::is_same_v<decltype(ItemType::value), uint64_t>);

    int num_streams = task->start_ptrs.size();
    std::vector<ItemType*> end_ptrs;
    std::vector<ItemType*> start_ptrs;
    task->total_records_sorted = 0;

    for (int i=0; i<num_streams; i++) {
        ItemType *end_ptr = (ItemType*) task->start_ptrs[i] + task->num_records[i];
        if (task->num_records[i] == 0) {
            end_ptrs.push_back(end_ptr);
            start_ptrs.push_back((ItemType*)task->start_ptrs[i]);
            continue;
        }

        uint64_t start_offset = (uint64_t(task->start_ptrs[i]) & (CACHE_LINE_SIZE - 1));
        uint64_t start_align = (start_offset == 0) ? 0 : (CACHE_LINE_SIZE - start_offset);
        uint64_t end_align = (uint64_t)end_ptr & (CACHE_LINE_SIZE - 1);
        spdlog::debug("Start align: {}, end align: {}, num_records: {}", start_align, end_align, task->num_records[i]);
        
        task->start_ptrs[i] += (start_align/sizeof(RecordType));
        end_ptr -= (end_align / sizeof(ItemType));

        end_ptrs.push_back(end_ptr);
        start_ptrs.push_back((ItemType*)task->start_ptrs[i]);
        task->total_records_sorted += task->num_records[i] - (start_align + end_align) / sizeof(RecordType);
        assert(std::is_sorted((ItemType*) task->start_ptrs[i], (ItemType*) end_ptr));
    }
    spdlog::debug("Number of elements after trimming unaligned ones: {}", task->total_records_sorted);

    int ret = posix_memalign(&task->output, 4096, task->total_records_sorted * sizeof(RecordType));
    assert(task->output != nullptr);

    uint32_t tree_height = std::log2(num_streams);
    std::unique_ptr<origami_merge_tree::MergeTree<Regtype, ItemType>> merge_tree;
    if (tree_height % 2 == 0) {
        merge_tree = std::make_unique<origami_merge_tree::MergeTreeEven<Regtype, ItemType>>();
    } else {
        merge_tree = std::make_unique<origami_merge_tree::MergeTreeOdd<Regtype, ItemType>>();
    }
    void *intermediate_buf;
    ret = posix_memalign(&intermediate_buf, 4096, MERGE_BUF_SIZE * 128);
    assert(ret == 0);
    merge_tree->merge_init(num_streams, (ItemType*)intermediate_buf, L1_CACHE_SIZE, L2_CACHE_SIZE);
    merge_tree->merge((ItemType**)start_ptrs.data(), (ItemType**)end_ptrs.data(), 
        (ItemType*)task->output, task->total_records_sorted, 
        L1_CACHE_SIZE, L2_CACHE_SIZE, 
        (ItemType*)intermediate_buf, num_streams);

    bool sorted = std::is_sorted((RecordType*)task->output, (RecordType*)task->output + task->total_records_sorted);
    *result_sorted = sorted;
}