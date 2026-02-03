#pragma once

#include "key_value_pair.h"
#include "spdlog/spdlog.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <utility>
#include <vector>

template <typename RecordType>
std::pair<std::vector<std::vector<RecordType*>>, std::vector<std::vector<uint64_t>>> generate_partitions(std::vector<RecordType*> start_ptrs, std::vector<uint64_t> lengths, int depth) {
    if (depth == 0) {
        std::vector<std::vector<RecordType*>> s = {start_ptrs};
        std::vector<std::vector<uint64_t>> l = {lengths};
        return std::make_pair(s, l);
    }
    assert(start_ptrs.size() == lengths.size());

    int num_arrays = start_ptrs.size();
    RecordType min_elem;
    min_elem.key = INT64_MAX;
    RecordType max_elem;
    max_elem.key = INT64_MIN;

    uint64_t total_elems = 0ll;
    for (int i=0; i<num_arrays; i++) {
        if (lengths[i] > 0) {
            if (start_ptrs[i][0] < min_elem) {
                min_elem = start_ptrs[i][0];
            }
            if (max_elem < start_ptrs[i][lengths[i]-1]) {
                max_elem = start_ptrs[i][lengths[i]-1];
            }
        }
        total_elems += lengths[i];
    }
    if (total_elems == 0) {
        std::vector<std::vector<RecordType*>> s = {start_ptrs};
        std::vector<std::vector<uint64_t>> l = {lengths};
        return std::make_pair(s, l);
    }

    while (min_elem < max_elem) {
        RecordType mid = mean<RecordType>(min_elem, max_elem);
        uint64_t count_le_mid = 0ll;
        for (int i=0; i<num_arrays; i++) {
            count_le_mid += std::upper_bound(start_ptrs[i], start_ptrs[i] + lengths[i], mid) - start_ptrs[i];
        }
        
        if (count_le_mid < total_elems / 2) {
            min_elem = increment<RecordType>(mid);
        } else {
            max_elem = mid;
        }
    }
    RecordType split_value = min_elem;

    std::vector<uint64_t> left_lengths(num_arrays);
    std::vector<uint64_t> right_lengths(num_arrays);

    std::vector<RecordType*> right_start_ptrs(num_arrays);
    for (int i=0; i<num_arrays; i++) {
        uint64_t idx = std::upper_bound(start_ptrs[i], start_ptrs[i] + lengths[i], split_value) - start_ptrs[i];
        left_lengths[i] = idx;
        right_lengths[i] = lengths[i] - idx;
        right_start_ptrs[i] = start_ptrs[i] + idx;
    }
    auto left_res = generate_partitions(start_ptrs, left_lengths, depth-1);
    auto right_res = generate_partitions(right_start_ptrs, right_lengths, depth-1);
    left_res.first.insert(left_res.first.end(), 
        std::make_move_iterator(right_res.first.begin()), 
        std::make_move_iterator(right_res.first.end()));
    left_res.second.insert(left_res.second.end(), 
        std::make_move_iterator(right_res.second.begin()), 
        std::make_move_iterator(right_res.second.end()));

    return std::make_pair(left_res.first, left_res.second);
}