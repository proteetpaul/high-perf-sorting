#pragma once

#include "io_task.h"

#include <cstdio>
#include <optional>
#include <cstring>
#include <deque>
#include <sys/types.h>
#include <vector>
#include <liburing.h>

class UringHandler {
public:
    UringHandler();

    void poll_for_completions(bool block);

    bool submit(IoTask task);

    bool can_submit() {
        printf("Num tokens: %d", tokens.size());
        return !tokens.empty();
    }

private:
    static constexpr int NUM_ENTRIES = 128;

    std::vector<IoTask> inflight_tasks;

    std::deque<uint32_t> tokens;

    io_uring ring;
};