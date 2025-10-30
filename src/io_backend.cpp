#include "io_backend.h"
#include "io_task.h"
#include "reader.h"

#include <cstdint>
#include <liburing.h>
#include <stdexcept>


UringHandler::UringHandler() {
    struct io_uring_params params;
    memset(&params, 0, sizeof(struct io_uring_params));
    params.flags = IORING_SETUP_SQPOLL;
    int ret = io_uring_queue_init_params(NUM_ENTRIES, &ring, &params);
    if (ret < 0) {
        throw std::runtime_error("Failed to create ring\n");
    }

    for (int i=0; i<NUM_ENTRIES; i++) {
        tokens.push_back(i);
    }
    inflight_tasks.resize(NUM_ENTRIES);
}

bool UringHandler::submit(IoTask task) {
    if (tokens.empty()) return false;
    io_uring_sqe *sqe = io_uring_get_sqe(&ring);

    uint32_t token = tokens.front();
    tokens.pop_front();
    if (task.type == TaskType::Read) {
        io_uring_prep_read(sqe, task.fd, task.buffer, task.length, task.offset);
    } else {
        io_uring_prep_write(sqe, task.fd, task.buffer, task.length, task.offset);
    }
    sqe->user_data = token;
    inflight_tasks[token] = std::move(task);
    io_uring_submit(&ring);
    return true;
}

void UringHandler::poll_for_completions(bool block) {
    struct io_uring_cqe *cqe;
    unsigned head;
    unsigned completions = 0;
    int ret = io_uring_wait_cqe_nr(&ring, &cqe, block);
    if (ret != 0) {
        throw std::runtime_error("IO completion returned error");
    }
    if (cqe != nullptr) {
        uint32_t token = cqe->user_data;
        tokens.push_back(token);
        IoTask task = std::move(inflight_tasks[token]);
        if (task.type == Read) {
            reinterpret_cast<SortedRunReader*>(task.requestor)->io_complete(&task);
        }
    }
}