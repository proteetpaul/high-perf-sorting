#pragma once

/**
 * Task 1: Fuse Phase 1 (read input) and Phase 2 (extract keys) using io_uring.
 *
 * Reads a run in small chunks via io_uring. When each chunk completes, keys
 * are extracted into (key, index) pairs and the chunk is copied into the run
 * buffer. Prefetching is done by submitting the next read as soon as a slot
 * is free (queue depth controls in-flight reads).
 */

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <linux/fs.h>
#include <memory>
#include <optional>
#include <queue>
#include <vector>

#include "config.h"
#include "merge.h"
#include "io_uring_utils.h"
#include "key_value_pair.h"
#include "spdlog/spdlog.h"


constexpr uint64_t DEFAULT_READ_CHUNK_BYTES = 512 * 1024;  // 512 KB

constexpr uint32_t PREFETCH_DEPTH = 4;

inline uint64_t align_up_block(uint64_t size) {
    const uint64_t mask = io_uring_utils::BLOCK_ALIGN - 1;
    return (size + mask) & ~mask;
}

/**
 * Extract keys from a chunk of raw record bytes into key_index_pairs.
 * Record layout: key (8 bytes) then value. Keys are byte-swapped to match
 * generate_key_index_pairs.
 */
template <typename RecordType>
void extract_keys_from_chunk(
    const RecordType* input_buf,
    uint64_t chunk_id,
    uint64_t num_records_in_chunk,
    KeyValuePair<RecordType::KEY_LENGTH, sizeof(uint64_t)>* key_index_pairs
) {
    constexpr uint32_t ELEM_SIZE = sizeof(RecordType);
    static_assert(RecordType::KEY_LENGTH == 8, "Size of key should be 8 bytes");

    const uint64_t num_records_per_full_chunk = DEFAULT_READ_CHUNK_BYTES / ELEM_SIZE;
    uint64_t idx = chunk_id * num_records_per_full_chunk;
    auto ptr = (RecordType*) input_buf + chunk_id * num_records_per_full_chunk;

    for (uint64_t i = 0; i < num_records_in_chunk; ++i) {
        key_index_pairs[idx].key = __builtin_bswap64(ptr->key);
        key_index_pairs[idx].set_value(&idx);
        ptr++;
        idx++;
    }
}

inline std::pair<uint64_t, int> process_cqe(io_uring_cqe *cqe) {
    int res = io_uring_utils::UringRing::cqe_result(cqe);
    uint64_t user_data = io_uring_utils::UringRing::cqe_user_data(cqe);
    return std::make_pair<>(user_data, res);
}

// TODO(): Rewrite this as a class
/**
 * Read one run from fd using io_uring in small chunks, and extract keys into
 * key_index_pairs. Run data is written into run_buffer (must be at least
 * run_size_bytes). key_index_pairs is resized and filled for the run.
 *
 * Uses a single io_uring ring with READ_QUEUE_DEPTH in-flight reads.
 * read_chunk_bytes: size of each read chunk (will be aligned to BLOCK_ALIGN).
 */
template <typename RecordType>
void read_run_and_extract_keys(int fd, uint64_t thread_id, uint64_t run_size_bytes,
    RecordType* records,
    KeyValuePair<RecordType::KEY_LENGTH, sizeof(uint64_t)>* key_index_pairs
) {
    using KeyIndexPair = KeyValuePair<RecordType::KEY_LENGTH, sizeof(uint64_t)>;
    constexpr uint32_t ELEM_SIZE = sizeof(RecordType);

    assert(Config::BLOCK_SIZE_ALIGN % ELEM_SIZE == 0);
    assert(run_size_bytes % ELEM_SIZE == 0);

    uint64_t num_records = run_size_bytes / ELEM_SIZE;
    uint64_t read_chunk_bytes = DEFAULT_READ_CHUNK_BYTES;

    uint64_t file_offset = thread_id * run_size_bytes;
    uint64_t num_chunks = (run_size_bytes + read_chunk_bytes - 1) / read_chunk_bytes;

    io_uring_utils::UringRing ring(PREFETCH_DEPTH);

    auto start = std::chrono::high_resolution_clock::now();
    uint64_t chunk_id = 0;
    uint64_t completed = 0;
    uint32_t free_slots = PREFETCH_DEPTH;

    while (completed < num_chunks) {
        while (chunk_id < num_chunks && free_slots > 0) {
            uint64_t chunk_offset = chunk_id * DEFAULT_READ_CHUNK_BYTES;
            auto *buf = records + chunk_offset / ELEM_SIZE;
            uint64_t chunk_bytes = std::min(DEFAULT_READ_CHUNK_BYTES, run_size_bytes - chunk_offset);
            uint64_t read_size = align_up_block(chunk_bytes);

            bool ok = ring.prepare_read(fd, (void*)buf, static_cast<uint32_t>(read_size),
                                      file_offset + chunk_offset,
                                      chunk_id << 32);
            if (!ok) {
                spdlog::error("submit_read failed for chunk {}", chunk_id);
                break;
            }
            free_slots--;
            chunk_id++;
        }
        ring.submit_and_wait(1);

        struct io_uring_cqe* cqe = nullptr;
        if (!ring.wait_cqe(&cqe)) {
            spdlog::error("wait_cqe failed");
            break;
        }

        auto p = process_cqe(cqe);
        uint32_t chunk_id = p.first >> 32;
        if (p.second < 0) {
            spdlog::error("read chunk {} failed: {}", chunk_id, p.second);
            ring.mark_cqe_seen(cqe);
            completed++;
            continue;
        }
        free_slots++;

        uint64_t done_chunk_offset = chunk_id * DEFAULT_READ_CHUNK_BYTES;
        uint64_t done_chunk_bytes = std::min(DEFAULT_READ_CHUNK_BYTES, run_size_bytes - done_chunk_offset);
        uint64_t num_records_chunk = done_chunk_bytes / ELEM_SIZE;

        extract_keys_from_chunk<RecordType>(
            records, chunk_id, num_records_chunk, key_index_pairs
        );
        ring.mark_cqe_seen(cqe);
        completed++;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;

    spdlog::debug("Run {}: read+extract {} ms, {} chunks", thread_id, time_elapsed, num_chunks);
}

/**
* Accumulates values into in-memory buffers and writes them out in batches. This ensures overlap between cpu and io, while keeping the memory footprint low.
*/
template<typename RecordType>
class ValueWriterPostSort {
    using KeyIndexPair = KeyValuePair<RecordType::KEY_LENGTH, sizeof(uint64_t)>;

    int fd;                         // Intermediate file containing values

    int thread_idx;            // Each thread writes to a non-overlapping portion of the file
    
    uint64_t values_per_chunk;      // Number of values per thread
    
    uint8_t *input_buffer;          // Buffer containing key-value pairs
    
    int run_idx;               // This writer is for the i'th sorted run
    
    std::vector<void*> write_bufs;  // Set of pre-allocated buffers used for writing out
    
    std::unique_ptr<io_uring_utils::UringRing> ring;

    KeyIndexPair* key_index_pairs;        // Sorted key-index pairs

    static constexpr uint32_t NUM_SLOTS = 16;

    static constexpr uint32_t BATCH_SIZE = 1;

    static constexpr uint64_t WRITE_IO_BYTES = RecordType::VALUE_LENGTH * io_uring_utils::BLOCK_ALIGN;
public:    
    explicit ValueWriterPostSort(int fd, int thread_idx, uint64_t values_per_chunk,
        uint8_t* input_buffer, int run_idx,
        KeyIndexPair* key_index_pairs
    ): thread_idx(thread_idx), values_per_chunk(values_per_chunk), 
            run_idx(run_idx), input_buffer(input_buffer), key_index_pairs(key_index_pairs) {
        this->fd = dup(fd);
        write_bufs.resize(NUM_SLOTS);
        for (uint32_t i=0; i<NUM_SLOTS; i++) {
            int ret = posix_memalign(&write_bufs[i], io_uring_utils::BLOCK_ALIGN, WRITE_IO_BYTES);
            assert(ret == 0);
            memset(write_bufs[i], 0, WRITE_IO_BYTES);
        }
    }

    ValueWriterPostSort(const ValueWriterPostSort&) = delete;
    ValueWriterPostSort& operator=(const ValueWriterPostSort&) = delete;

    ~ValueWriterPostSort() {
        if (fd >= 0) {
            close(fd);
        }
        for (uint32_t i = 0; i < write_bufs.size(); i++) {
            free(write_bufs[i]);
        }
    }

    /** Fills write_bufs[buf_slot] with values for key_index_pairs[start_index .. start_index+num_values-1]. */
    void write_values_to_buf(uint32_t buf_slot, uint64_t start_index) {
        const uint64_t num_values = WRITE_IO_BYTES / RecordType::VALUE_LENGTH;
        uint8_t *buf = (uint8_t*)write_bufs[buf_slot];
        for (uint64_t i = 0; i < num_values; i++) {
            KeyIndexPair &kv = key_index_pairs[start_index + i];
            const void *value_ptr = input_buffer + kv.value * sizeof(RecordType) + RecordType::KEY_LENGTH;
            std::memcpy(buf, value_ptr, RecordType::VALUE_LENGTH);
            kv.value = (uint64_t) run_idx;
            buf += RecordType::VALUE_LENGTH;
        }
    }

    void run() {
        spdlog::debug("Start writing out values post sort");
        ring = std::make_unique<io_uring_utils::UringRing>(NUM_SLOTS);

        std::queue<uint32_t> slots;
        for (uint32_t i=0; i<NUM_SLOTS; i++) {
            slots.push(i);
            // TODO(): Integrate fixed buffers here
        }
        uint64_t bytes_to_write = values_per_chunk * RecordType::VALUE_LENGTH;
        assert(bytes_to_write % WRITE_IO_BYTES == 0);
    
        const uint64_t num_values_per_batch = WRITE_IO_BYTES / RecordType::VALUE_LENGTH;
        uint64_t num_writes = bytes_to_write / WRITE_IO_BYTES;
        uint64_t completed = 0ll;
        uint32_t to_submit = 0;
        uint64_t next_write = 0ll;
        uint64_t file_offset = thread_idx * bytes_to_write;
        while (completed < num_writes) {
            if (!slots.empty() && next_write < num_writes) {
                uint32_t slot = slots.front();
                slots.pop();
                uint64_t start_index = next_write * num_values_per_batch;
                write_values_to_buf(slot, start_index);
                ring->prepare_write(fd, write_bufs[slot], WRITE_IO_BYTES, file_offset, slot);
                to_submit++;
                next_write++;
                file_offset += WRITE_IO_BYTES;
            }

            bool should_submit = slots.empty() || to_submit >= BATCH_SIZE || (next_write == num_writes);
            if (should_submit) {
                int nr = (next_write == num_writes) ? static_cast<int>(num_writes - completed) : slots.empty();
                ring->submit_and_wait(nr);
                to_submit = 0;

                struct io_uring_cqe* cqe = nullptr;
                while (ring->peek_cqe(&cqe)) {
                    auto p = process_cqe(cqe);
                    uint32_t slot_id = p.first;
                    if (p.second < 0) {
                        spdlog::error("write chunk failed: {}", p.second);
                    }
                    slots.push(slot_id);
                    completed++;
                    ring->mark_cqe_seen(cqe);
                }
            }
        }
        spdlog::debug("Done writing out values post sort");
    }
};

class AsyncValueReader {
    enum BufState {
        Empty,
        IoCompleted,
        WaitingForIO,
    };

    int fd;
    uint64_t file_offset;    // Offset within the file
    uint64_t end_file_offset;
    int reader_id;
    
    // TODO(): Maybe the number of buffers can be generalized??
    void *ptr[2];
    BufState states[2];
    uint64_t chunk_offset;      // Offset within the in-memory buffer that is being used for reads
    uint64_t read_chunk_size;
    uint64_t value_length_bytes;    // Size of individual values
    uint64_t cur_buf_idx;

public:
    inline bool waiting_for_io() {
        // spdlog::debug("cur_buf_idx of stream {}: {}", reader_id, cur_buf_idx);
        return states[cur_buf_idx] != BufState::IoCompleted;
    }

    AsyncValueReader(int fd, uint64_t start_offset, uint64_t value_length, uint64_t read_chunk_size, int reader_id): 
        fd(fd), file_offset(start_offset), chunk_offset(0ll), value_length_bytes(value_length), reader_id(reader_id),
        read_chunk_size(read_chunk_size) {
        for (int i=0; i<2; i++) {
            int ret = posix_memalign(&ptr[i], 4096, read_chunk_size);
            assert(ret == 0);
            memset(ptr[i], 0, read_chunk_size);
            states[i] = BufState::Empty;
        }
        cur_buf_idx = 0;
    }

    AsyncValueReader(const AsyncValueReader&) = delete;
    AsyncValueReader& operator=(const AsyncValueReader&) = delete;

    ~AsyncValueReader() {
        close(fd);
        free(ptr[0]);
        free(ptr[1]);
    }

    inline void *get_next_value_fast() {
        if (states[cur_buf_idx] != BufState::IoCompleted || chunk_offset >= read_chunk_size) {
            return nullptr;
        }
        void *res = (uint8_t*) ptr[cur_buf_idx] + chunk_offset;
        chunk_offset += value_length_bytes;
        return res;
    }

    inline bool need_submit() {
        return states[cur_buf_idx] == BufState::IoCompleted || states[cur_buf_idx ^ 1] == BufState::Empty;
    }

    inline void *get_next_value() {
        if (states[cur_buf_idx] != BufState::IoCompleted) {
            return nullptr;
        }
        if (chunk_offset >= read_chunk_size) {
            states[cur_buf_idx] = BufState::Empty;

            cur_buf_idx = 1 ^ cur_buf_idx;
            chunk_offset = 0ll;
            if (states[cur_buf_idx] != BufState::IoCompleted) {
                return nullptr;
            }
        }
        void *res = (uint8_t*) ptr[cur_buf_idx] + chunk_offset;
        chunk_offset += value_length_bytes;
        return res;
    }

    inline void process_io_completion(uint64_t user_data) {
        int buf_idx = (int)(user_data & 1);
        // spdlog::debug("buf_idx of completed IO: {}", buf_idx);
        states[buf_idx] = IoCompleted;
    }

    inline std::optional<io_uring_utils::ReadTask> get_next_io() {
        uint64_t next_buf_idx = (states[cur_buf_idx] == Empty) ? cur_buf_idx: cur_buf_idx ^ 1;
        // uint64_t next_buf_idx = cur_buf_idx ^ 1;
        if (states[next_buf_idx] == IoCompleted) {
            return std::nullopt;
        }
        states[next_buf_idx] = WaitingForIO;
        uint64_t user_data = (reader_id << 16) | next_buf_idx;
        io_uring_utils::ReadTask task {
            ptr[next_buf_idx], read_chunk_size, 
            fd, file_offset, user_data
        };
        file_offset += read_chunk_size;
        return task;
    }
};


template <typename RecordType>
class ValueWriterPostMerge {
    using KeyIndexPair = KeyValuePair<RecordType::KEY_LENGTH, sizeof(uint64_t)>;

    static constexpr uint64_t WRITE_IO_BYTES = 120 * 1024;

    static constexpr uint64_t READ_IO_CHUNK = 120 * 1024;

    int out_fd;                         // Output file containing key-value pairs
    
    MergeTask<KeyIndexPair> *task;
    
    std::vector<void*> write_bufs;      // Set of pre-allocated buffers used for writing out

    std::queue<uint32_t> slots;
    
    std::unique_ptr<io_uring_utils::UringRing> ring;

    std::vector<std::unique_ptr<AsyncValueReader>> readers;

public:
    static constexpr uint64_t NUM_SLOTS = PREFETCH_DEPTH * 4;

    static constexpr uint32_t BATCH_SIZE = 1;

    explicit ValueWriterPostMerge(MergeTask<KeyIndexPair> *task, int out_fd, 
        std::vector<int> &in_fds, std::vector<KeyIndexPair*> &start_ptrs)
            : task(task) {
        assert(in_fds.size() == start_ptrs.size());
        assert(in_fds.size() > 0 && "in_fds must not be empty");
        assert(WRITE_IO_BYTES % RecordType::VALUE_LENGTH == 0);
        
        this->out_fd = dup(out_fd);

        for (int i=0; i<in_fds.size(); i++) {
            int fd = dup(in_fds[i]);
            uint64_t file_offset = (start_ptrs[i] - task->start_ptrs[i]) * RecordType::VALUE_LENGTH;
            auto reader = std::make_unique<AsyncValueReader>(fd, file_offset, RecordType::VALUE_LENGTH, READ_IO_CHUNK, i);
            readers.push_back(std::move(reader));
        }

        write_bufs.resize(NUM_SLOTS);
        for (uint32_t i=0; i<NUM_SLOTS; i++) {
            int ret = posix_memalign(&write_bufs[i], io_uring_utils::BLOCK_ALIGN, WRITE_IO_BYTES);
            assert(ret == 0);
            memset(write_bufs[i], 0, WRITE_IO_BYTES);
        }
    }

    ~ValueWriterPostMerge() {
        close(out_fd);
        for (void *buf: write_bufs) {
            free(buf);
        }
    }

    ValueWriterPostMerge(const ValueWriterPostMerge&) = delete;
    ValueWriterPostMerge& operator=(const ValueWriterPostMerge&) = delete;

    inline void poll_completions() {
        // spdlog::debug("Polling completions");
        struct io_uring_cqe *cqe;
        if (!ring->peek_cqe(&cqe)) {
            ring->wait_cqe(&cqe);
        }

        if ((cqe->user_data >> 32) < NUM_SLOTS) {
            // spdlog::debug("Processing write completion");
            uint32_t slot = (cqe->user_data >> 32);
            slots.push(slot);
        } else {
            int reader_idx = (cqe->user_data >> 16) & 0xffff;
            // spdlog::debug("Reader idx: {}", reader_idx);
            readers[reader_idx]->process_io_completion(cqe->user_data);
            auto task = readers[reader_idx]->get_next_io();
            if (task.has_value()) {
                task.value().user_data |= (NUM_SLOTS << 32);
                ring->prepare_read(task.value());
            }
        }
        ring->mark_cqe_seen(cqe);
    }

    void run() {
        spdlog::debug("Start post-merge ops");
        uint32_t ring_num_entries = NUM_SLOTS + readers.size();
        ring = std::make_unique<io_uring_utils::UringRing>(ring_num_entries);

        for (uint32_t i=0; i<NUM_SLOTS; i++) {
            slots.push(i);
        }
        uint64_t records_emitted = 0ll;
        KeyIndexPair *sorted_keys = (KeyIndexPair*)task->output;

        for (auto &reader: readers) {
            auto task = reader->get_next_io();
            if (task.has_value()) {
                task.value().user_data |= (NUM_SLOTS << 32);
                ring->prepare_read(task.value());
            }
        }
        ring->submit_and_wait(0);
        uint64_t out_file_offset = 0ll;

        while (records_emitted < task->total_records_sorted) {
            while (slots.empty()) {
                poll_completions();                
            }
            uint32_t slot = slots.front();
            slots.pop();
            uint64_t num_records_in_batch = std::min(task->total_records_sorted - records_emitted, WRITE_IO_BYTES / sizeof(RecordType));
            RecordType *out_buf = reinterpret_cast<RecordType*>(write_bufs[slot]);
            uint64_t offset = 0;

            for (uint64_t i=0; i<num_records_in_batch; i++) {
                out_buf[i].key = __builtin_bswap64(sorted_keys->key);
                uint32_t stream_id = sorted_keys->value;

                assert(stream_id < readers.size());
                assert(readers[stream_id] != nullptr);

                void *value = readers[stream_id]->get_next_value_fast();
                if (value == nullptr) [[unlikely]] {
                    // Having another call here just so that we can submit an io pre-emptively. This is necessary when switching from
                    // one buffer to the next. If the IO to the next buffer is complete, it won't refill the earlier buffer.
                    value = readers[stream_id]->get_next_value();
                    if (readers[stream_id]->need_submit()) {
                        auto task = readers[stream_id]->get_next_io();
                        assert(task.has_value() && "Task is null");
                        task.value().user_data |= (NUM_SLOTS << 32);
                        ring->prepare_read(task.value());
                    }

                    while (readers[stream_id]->waiting_for_io()) {
                        poll_completions();
                    }
                    ring->submit_and_wait(0);
                    value = readers[stream_id]->get_next_value();
                }
                assert(value != nullptr && "Value is null");
                std::memcpy(&out_buf[i].value, value, RecordType::VALUE_LENGTH);
                sorted_keys++;
            }
            ring->prepare_write(out_fd, write_bufs[slot], num_records_in_batch * sizeof(RecordType), 
                out_file_offset, (uint64_t)slot << 32);
            ring->submit_and_wait(0);
            out_file_offset += num_records_in_batch * sizeof(RecordType);
            records_emitted += num_records_in_batch;
        }
        spdlog::debug("End post-merge ops");
    }
};
