#include "../../src/key_index_pair.h"
#include "node.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <cstdlib>
#include <thread>
#include <pthread.h>
#include <chrono>
#include <vector>

long get_file_size(const char *filename) {
    struct stat file_status;
    if (stat(filename, &file_status) < 0) {
        return -1;
    }

    return file_status.st_size;
}

template<typename T>
class MergeBenchmark {
    std::vector<std::string> paths;

public:
    MergeBenchmark(std::string file_prefix, uint32_t num_files) {
        paths.reserve(num_files);
        for (int i=0; i<num_files; i++) {
            std::string path = file_prefix + "-chunk-" + std::to_string(i);
            paths.push_back(path);
        }
    }

    std::vector<Block> load_input() {
        std::vector<Block> result;
        for (int i=0; i<paths.size(); i++) {
            void *input_buffer;
            int fd = open(paths[i].c_str(), O_RDONLY | O_DIRECT);
            long size_bytes = get_file_size(paths[i].c_str());
            printf("Size bytes: %d\n", (int)size_bytes);
            int ret = posix_memalign(&input_buffer, 4096, size_bytes);
            assert(ret == 0);
            ret = pread64(fd, input_buffer, size_bytes, 0);
            result.push_back(Block {input_buffer, (uint64_t) size_bytes});
        }
        return result;
    }

    void run() {
        auto input_blocks = load_input();
        std::vector<MergeNode<T>> merge_tree = construct_merge_tree<T>(input_blocks);
        uint32_t num_internal_nodes = merge_tree.size();
        std::vector<std::thread> threads;
        pthread_barrier_t barrier;
        int s = pthread_barrier_init(&barrier, NULL, num_internal_nodes);

        auto fn = [](MergeNode<T> *node, pthread_barrier_t *barrier) {
            pthread_barrier_wait(barrier);
            node->run();
        };

        for (int i=0; i<num_internal_nodes; i++) {
            threads.emplace_back(fn, &merge_tree[i], &barrier);
        }
        for (int i=0; i<num_internal_nodes; i++) {
            threads[i].join();
        }
    }

};

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <key_size> <value_size> <intermediate_file_prefix> <num_files>" << std::endl;
        return 1;
    }
    
    size_t key_size = std::stoull(argv[1]);
    size_t value_size = std::stoull(argv[2]);
    std::string intermediate_file_prefix = argv[3];
    uint32_t num_files = std::stoi(argv[4]);
    
    // Instantiate MergeBenchmark based on key_size and value_size
    // Following the pattern from src/main.cpp
    if (key_size == 8 && value_size == 8) {
        MergeBenchmark<KeyValuePair<8, 8>> benchmark(intermediate_file_prefix, num_files);
        benchmark.run();
    } 
    else if (key_size == 8 && value_size == 24) {
        MergeBenchmark<KeyValuePair<8, 24>> benchmark(intermediate_file_prefix, num_files);
        benchmark.run();
    } 
    else if (key_size == 8 && value_size == 56) {
        MergeBenchmark<KeyValuePair<8, 56>> benchmark(intermediate_file_prefix, num_files);
        benchmark.run();
    } 
    else if (key_size == 8 && value_size == 120) {
        MergeBenchmark<KeyValuePair<8, 120>> benchmark(intermediate_file_prefix, num_files);
        benchmark.run();
    } 
    else if (key_size == 8 && value_size == 0) {
        MergeBenchmark<KeyValuePair<8, 0>> benchmark(intermediate_file_prefix, num_files);
        benchmark.run();
    } 
    else {
        std::cerr << "Error: Unsupported (key_size, value_size) combination: (" 
                  << key_size << ", " << value_size << ")" << std::endl;
        return 1;
    }
    
    return 0;
}