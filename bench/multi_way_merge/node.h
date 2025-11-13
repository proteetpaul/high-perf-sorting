#pragma once
#include <algorithm>
#include <boost/lockfree/spsc_queue.hpp>
#include <cassert>
#include <cstdint>
#include <memory>
#include <cstring>
#include <sys/types.h>
#include <unistd.h>
#include <utility>
#include <stdio.h>
#include <spdlog/spdlog.h>

struct Block {
    void *ptr;
    uint64_t size;
};

using spsc_queue = boost::lockfree::spsc_queue<Block>;

struct BaseBlockReader {
    virtual Block read_block() = 0;
    
    BaseBlockReader() {}
};

struct MemoryBlockReader : public BaseBlockReader {
    Block buffer;
    bool read;

    MemoryBlockReader(Block blk): buffer(blk), BaseBlockReader(), read(false) {}

    virtual Block read_block() {
        if (read) return Block {nullptr, 0};
        read = true;
        return buffer;
    }
};

struct QueueBlockReader: public BaseBlockReader {
    std::shared_ptr<spsc_queue> queue;
    std::shared_ptr<spsc_queue> free_queue;
    Block *current;
    
    QueueBlockReader(std::shared_ptr<spsc_queue> queue, std::shared_ptr<spsc_queue> free_queue)
        : queue(queue), free_queue(free_queue), BaseBlockReader(), current(nullptr) {}

    virtual Block read_block() {
        // spdlog::info("Inside QueueBlockReader::read_block()");
        if (current != nullptr) {
            while (!free_queue->write_available()) {
                usleep(1000);
            }
            free_queue->push(*current);
        } else {
            current = new Block();
        }
        // spdlog::info("waiting to read block from queue");
        while (!queue->read_available()) {
            usleep(1000);
        }
        queue->pop(*current);
        return *current;
    }
};

struct BaseBlockWriter {
    virtual void write_block(Block &blk) = 0;

    virtual Block get_empty_block() = 0;
    
    BaseBlockWriter() {}
};

struct MemoryBlockWriter : public BaseBlockWriter {
    Block buffer;

    MemoryBlockWriter(Block blk): buffer(blk), BaseBlockWriter() {}

    virtual void write_block(Block &blk) {}

    virtual Block get_empty_block() {
        return buffer;
    }
};

struct QueueBlockWriter: public BaseBlockWriter {
    std::shared_ptr<spsc_queue> queue;
    std::shared_ptr<spsc_queue> free_queue;
    
    QueueBlockWriter(std::shared_ptr<spsc_queue> queue, std::shared_ptr<spsc_queue> free_queue)
    : queue(queue), free_queue(free_queue), BaseBlockWriter() {}

    virtual Block get_empty_block() {
        while (!free_queue->read_available()) {
            usleep(1000);
        }
        Block blk;
        free_queue->pop(blk);
        return blk;
    }

    virtual void write_block(Block &blk) {
        while (!queue->write_available()) {
            usleep(1000);
        }
        queue->push(blk);
    }
};


template <typename T>
struct MergeNode {
    std::shared_ptr<BaseBlockReader> input_reader1;
    std::shared_ptr<BaseBlockReader> input_reader2;
    uint32_t id;
    
    std::shared_ptr<BaseBlockWriter> output;

    MergeNode(std::shared_ptr<BaseBlockReader> &input1, std::shared_ptr<BaseBlockReader> &input2, 
            std::shared_ptr<BaseBlockWriter> &output, uint32_t id)
        : input_reader1(input1), input_reader2(input2), output(output), id(id) {}

    void run() {
        Block input_blk1 = input_reader1->read_block();
        Block input_blk2 = input_reader2->read_block();
        Block output_blk = output->get_empty_block();

        spdlog::info("Input1.size: %ld, Input2.size: %ld, output.size: %ld \n", input_blk1.size, input_blk2.size, output_blk.size);

        uint64_t input1_offset, input2_offset, output_offset;
        input1_offset = input2_offset = output_offset = 0ll;
        bool i1_completed, i2_completed;
        i1_completed = i2_completed = false;

        while (true) {
            if (input1_offset >= input_blk1.size) {
                // spdlog::info("Node {} waiting to read block\n", id);
                input_blk1 = input_reader1->read_block();
                if (input_blk1.ptr == nullptr) {
                    i1_completed = true;
                    break;
                }
                input1_offset = 0;
            }
            if (input2_offset >= input_blk2.size) {
                // spdlog::info("Node {} waiting to read block\n", id);
                input_blk2 = input_reader2->read_block();
                if (input_blk2.ptr == nullptr) {
                    i2_completed = true;
                    break;
                }
                input2_offset = 0;
            }
            if (output_offset >= output_blk.size) {
                // spdlog::info("Node {} waiting to write block\n", id);
                output->write_block(output_blk);
                output_blk = output->get_empty_block();
                // spdlog::info("Node {} wrote block\n", id);
                output_offset = 0ll;
            }
            T* ptr1 = reinterpret_cast<T*>((uint8_t*)input_blk1.ptr + input1_offset);
            T* ptr2 = reinterpret_cast<T*>((uint8_t*)input_blk2.ptr + input2_offset);
            bool b = (*ptr1 < *ptr2);
            T** ptr = b ? &ptr1: &ptr2;
            std::memcpy((void*)((uint8_t*)output_blk.ptr + output_offset), (void*)(*ptr), sizeof(T));
            input1_offset += b ? sizeof(T): 0;
            input2_offset += b ? 0: sizeof(T);
            output_offset += sizeof(T);
        }
        spdlog::info("Node {} Completed while loop", id);

        auto write_out_input = [&output_offset, this, &output_blk](uint64_t input_offset, Block input_blk, 
                std::shared_ptr<BaseBlockReader> &input) {
            bool completed = false;
            while (!completed) {
                if (input_offset >= input_blk.size) {
                    input_blk = input->read_block();
                    if (input_blk.ptr == nullptr) {
                        completed = true;
                        break;
                    }
                    input_offset = 0;
                }
                if (output_offset >= output_blk.size) {
                    output->write_block(output_blk);
                    output_blk = output->get_empty_block();
                    output_offset = 0ll;
                }
                uint64_t input_remaining = (input_blk.size - input_offset);
                uint64_t output_remaining = (output_blk.size - output_offset);
                uint64_t bytes_to_write = std::min(input_remaining, output_remaining);
                std::memcpy((void*)((uint8_t*)output_blk.ptr + output_offset), (void*)((uint8_t*)input_blk.ptr + input_offset), sizeof(T));
                output_offset += bytes_to_write;
                input_offset += bytes_to_write;
            }
        };
        if (!i1_completed) {
            write_out_input(input1_offset, input_blk1, input_reader1);
        }
        if (!i2_completed) {
            write_out_input(input2_offset, input_blk2, input_reader2);
        }

        if (output_offset > 0) {
            output_blk.size = output_offset;
            output->write_block(output_blk);
        }
        if (id == 0) {
            spdlog::info("Output offset: {}", output_offset);
        }
        Block null_block {nullptr, 0};
        output->write_block(null_block);
    }
};

using queue_pair = std::pair<std::shared_ptr<spsc_queue>, std::shared_ptr<spsc_queue>>;

queue_pair create_queue_pair(uint64_t block_size, uint32_t element_count) {
    std::shared_ptr<spsc_queue> queue = std::make_shared<spsc_queue>(element_count);
    std::shared_ptr<spsc_queue> free_queue = std::make_shared<spsc_queue>(element_count);
    for (int i=0; i<element_count; i++) {
        void *block_ptr;
        int ret = posix_memalign(&block_ptr, 64, block_size);
        assert(ret == 0);
        free_queue->push(Block {block_ptr, block_size});
    }
    return std::make_pair(queue, free_queue);
}

uint32_t QUEUE_SIZE = 64;
uint64_t BLOCK_SIZE = 1024 * 1024;

template <typename RecordType>
std::vector<MergeNode<RecordType>> construct_merge_tree(std::vector<Block> &input_blocks) {
    uint32_t num_inputs = input_blocks.size();
    uint32_t next_power_of_2 = 1;
    while (next_power_of_2 < num_inputs) {
        next_power_of_2 *= 2;
    }
    uint32_t num_nodes;
    if (next_power_of_2 == num_inputs) {
        num_nodes = 2 * next_power_of_2 - 1;
    } else {
        num_nodes = next_power_of_2 - 2 * (next_power_of_2 - num_inputs) + next_power_of_2 - 1; 
    }
    uint32_t num_internal_nodes = num_nodes - num_inputs;
    printf("Num internal nodes: %d\n", num_internal_nodes);
    std::vector<MergeNode<RecordType>> merge_tree;

    uint64_t output_size = 0ll;
    for (auto& block: input_blocks) {
        output_size += block.size;
    }
    void *output_buffer;
    int ret = posix_memalign(&output_buffer, 4096, output_size);
    assert(ret == 0);
    Block output_block {output_buffer, output_size};

    for (int i=0; i<num_internal_nodes; i++) {
        std::shared_ptr<BaseBlockWriter> output;
        std::shared_ptr<BaseBlockReader> input1, input2;

        if (i == 0) {
            // Root node
            output = std::make_shared<MemoryBlockWriter>(output_block);
        } else {
            // writer should write to parent's input
            int parent = (i - 1)/2;
            MergeNode<RecordType> parent_node = merge_tree[parent];
            bool is_left_child = ((2 * parent + 1) == i);
            std::shared_ptr<QueueBlockReader> parent_block_reader;
            if (is_left_child) {
                parent_block_reader = std::dynamic_pointer_cast<QueueBlockReader>(parent_node.input_reader1);
            } else {
                parent_block_reader = std::dynamic_pointer_cast<QueueBlockReader>(parent_node.input_reader2);
            }
            output = std::make_shared<QueueBlockWriter>(parent_block_reader->queue, parent_block_reader->free_queue);
        }

        int left_child = 2 * i + 1;
        int right_child = 2 * i + 2;
        if (left_child < num_internal_nodes) {
            auto queue_pair = create_queue_pair(BLOCK_SIZE, QUEUE_SIZE);
            input1 = std::make_shared<QueueBlockReader>(queue_pair.first, queue_pair.second);
        } else {
            uint32_t input_idx = left_child - num_internal_nodes;
            input1 = std::make_shared<MemoryBlockReader>(input_blocks[input_idx]);
        }

        if (right_child < num_internal_nodes) {
            auto queue_pair = create_queue_pair(BLOCK_SIZE, QUEUE_SIZE);
            input2 = std::make_shared<QueueBlockReader>(queue_pair.first, queue_pair.second);
        } else {
            uint32_t input_idx = right_child - num_internal_nodes;
            input2 = std::make_shared<MemoryBlockReader>(input_blocks[input_idx]);
        }
        
        MergeNode<RecordType> merge_node (input1, input2, output, i);
        merge_tree.push_back(merge_node);
    }
    return merge_tree;
}

