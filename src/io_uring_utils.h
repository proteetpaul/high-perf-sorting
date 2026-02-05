#pragma once

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <liburing.h>
#include <unistd.h>
#include <sys/syscall.h>

namespace io_uring_utils {

constexpr uint32_t BLOCK_ALIGN = 4096;

constexpr bool POLL = true;

struct ReadTask {
    void *buf;
    uint64_t bytes;
    int fd;
    uint64_t offset;
    uint64_t user_data;
};

/**
 * Per-thread io_uring context
 */
struct UringRing {
    struct io_uring ring_;
    uint32_t queue_depth_;
    uint32_t pending_;  // number of submitted but not yet reaped

    explicit UringRing(uint32_t queue_depth)
        : queue_depth_(queue_depth), pending_(0) {
        unsigned int flags = 0;
        // flags |= IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_SINGLE_ISSUER;
        if (POLL) {
            flags |= IORING_SETUP_IOPOLL;
        } 
        
        int ret = io_uring_queue_init(queue_depth, &ring_, flags);
        assert(ret >= 0 && "io_uring_queue_init failed");
    }

    ~UringRing() {
        io_uring_queue_exit(&ring_);
    }

    UringRing(const UringRing&) = delete;
    UringRing& operator=(const UringRing&) = delete;

    struct io_uring* get() { return &ring_; }

    bool prepare_read(int fd, void* buf, uint32_t len, uint64_t file_offset, uint64_t user_data) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) return false;
        io_uring_prep_read(sqe, fd, buf, len, file_offset);
        io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(static_cast<uintptr_t>(user_data)));
        io_uring_sqe_set_flags(sqe, 0);
        pending_++;
        return true;
    }

    bool prepare_read(ReadTask &task) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) return false;
        io_uring_prep_read(sqe, task.fd, task.buf, task.bytes, task.offset);
        io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(static_cast<uintptr_t>(task.user_data)));
        io_uring_sqe_set_flags(sqe, 0);
        pending_++;
        return true;
    }

    bool prepare_write(int fd, void *buf, uint32_t len, uint64_t file_offset, uint64_t user_data) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) return false;
        io_uring_prep_write(sqe, fd, buf, len, file_offset);
        io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(static_cast<uintptr_t>(user_data)));
        io_uring_sqe_set_flags(sqe, 0);
        pending_++;
        return true;
    }

    /** Submit previously prepared SQEs to the kernel. */
    int submit_and_wait(int wait_nr) {
        int n = io_uring_submit_and_wait(&ring_, wait_nr);
        return n;
    }

    bool wait_cqe(struct io_uring_cqe** cqe_ptr) {
        int ret = io_uring_wait_cqe(&ring_, cqe_ptr);
        if (ret < 0) return false;
        return true;
    }

    /**
     * Peek at a completion without removing it. Returns true if one is available.
     */
    bool peek_cqe(struct io_uring_cqe** cqe_ptr) {
        return io_uring_peek_cqe(&ring_, cqe_ptr) == 0;
    }

    void mark_cqe_seen(struct io_uring_cqe* cqe) {
        io_uring_cqe_seen(&ring_, cqe);
        if (pending_ > 0) pending_--;
    }

    static uint64_t cqe_user_data(struct io_uring_cqe* cqe) {
        return static_cast<uint64_t>(reinterpret_cast<uintptr_t>(io_uring_cqe_get_data(cqe)));
    }

    static int cqe_result(struct io_uring_cqe* cqe) {
        return cqe->res;
    }
};

/**
 * Allocate a buffer suitable for O_DIRECT and io_uring reads.
 * Caller must free with aligned_free.
 */
inline void* aligned_alloc_read_buffer(uint64_t size) {
    assert(size > 0 && (size % BLOCK_ALIGN) == 0);
    void* p = nullptr;
    int ret = posix_memalign(&p, BLOCK_ALIGN, size);
    assert(ret == 0);
    return p;
}

}  // namespace io_uring_utils
