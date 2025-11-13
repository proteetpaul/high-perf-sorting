#pragma once
#include <vector>

template <typename T>
struct spsc_queueDeque {
    int capacity;
    int head_offset;
    int tail_offset;
    void *base_ptr;
    std::vector<T> items;

    Deque(int capacity): capacity(capacity), head_offset(0), tail_offset(0) {
        items.reserve(capacity);
    }

    void push(T item) {

    }

    T pop() {

    }

    T is_empty() {

    }
};