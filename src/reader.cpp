#include "reader.h"
#include <cstdio>

SortedRunReader::SortedRunReader(uint64_t buffer_capacity, SortedRun run, int elem_size,
    std::shared_ptr<UringHandler> handler)
        : run(run), elem_size(elem_size), io_handler(handler) {
    chunk_size = ((buffer_capacity / NUM_BUFFERS) >> 12) << 12;     // Ensure 4096 byte alignment
    processed = 0;
    state = ReaderState::WaitingForIO;
    num_chunks = (run.num_elements * elem_size + chunk_size - 1) / chunk_size;
    chunk_to_buffer_idx_mapping.resize(NUM_BUFFERS);
    next_chunk = 0;
    buffer_offset = 0;

    all_buffers.reserve(NUM_BUFFERS);
    for (int i=0; i<NUM_BUFFERS; i++) {
        chunk_to_buffer_idx_mapping[i] = -1;
        void *buffer;
        int ret = posix_memalign(&buffer, 4096, chunk_size);
        assert(ret == 0);

        all_buffers.push_back(buffer);
        ready_for_io.push_back(i);
    }

    submit_next_io_task();
}

void SortedRunReader::submit_next_io_task() {
    IoTask task;
    task.buffer = nullptr;
    if (ready_for_io.empty() || next_chunk == num_chunks) {
        return;
    }

    uint32_t next_buffer_idx = ready_for_io.front();
    ready_for_io.pop_front();
    task.buffer = all_buffers[next_buffer_idx];
    task.offset = next_chunk * chunk_size;
    task.length = std::min(chunk_size, run.num_elements * elem_size - task.offset);
    task.fd = run.fd;
    task.data = next_buffer_idx;
    task.type = TaskType::Read;
    task.requestor = this;
    if (io_handler->can_submit()) {
        printf("Submitting io task for reader\n");
        io_handler->submit(task);
    }
    ++next_chunk;
}

void SortedRunReader::io_complete(IoTask *task) {
    chunk_to_buffer_idx_mapping[(task->offset / chunk_size) % NUM_BUFFERS] = task->data;
    state = ReaderState::Ready;
}

void *SortedRunReader::next() {
    while (state == WaitingForIO) {
        io_handler->poll_for_completions(true);
    }
    if (buffer_offset + elem_size > chunk_size) {
        submit_next_io_task();
        ready_for_io.push_back(chunk_to_buffer_idx_mapping[current_chunk % NUM_BUFFERS]);
        current_chunk++;
        auto buffer_idx = chunk_to_buffer_idx_mapping[current_chunk % NUM_BUFFERS];
        if (buffer_idx == -1) {
            state = ReaderState::WaitingForIO;
            while (state == WaitingForIO) {
                io_handler->poll_for_completions(true);
            }
            return nullptr;
        }
        current_buffer = all_buffers[buffer_idx];
        buffer_offset = 0;
    }
    char *buffer_ptr = (char*)current_buffer + buffer_offset;
    processed++;
    buffer_offset += elem_size;
    return buffer_ptr;
}