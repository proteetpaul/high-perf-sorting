#pragma once

#include <cstdint>

enum TaskType {
    Read,
    Write,
};

struct IoTask {
    void *buffer;

    void *requestor;

    uint64_t offset;

    uint64_t length;

    int fd;

    uint32_t data;

    TaskType type;
};