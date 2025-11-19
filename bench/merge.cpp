#include "../src/key_value_pair.h"

#include <cassert>
#include <cstring>
#include <string>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <cstdlib>
#include <chrono>

long get_file_size(const char *filename) {
    struct stat file_status;
    if (stat(filename, &file_status) < 0) {
        return -1;
    }

    return file_status.st_size;
}

template<typename T>
class MergeBenchmark {
    std::string path1;
    std::string path2;
public:
    MergeBenchmark(std::string file_prefix) {
        path1 = file_prefix + "-chunk-0";
        path2 = file_prefix + "-chunk-1";
    }

    void run() {
        const int ELEM_SIZE = sizeof(T);
        struct stat file_status;
        long size_bytes = get_file_size(path1.c_str());
        printf("Size bytes: %ld\n", size_bytes);
        int fd1 = open(path1.c_str(), O_RDONLY | O_DIRECT);
        int fd2 = open(path2.c_str(), O_RDONLY | O_DIRECT);

        void *input1, *input2, *output;
        int ret = posix_memalign(&input1, 4096, size_bytes);
        assert(ret == 0);
        ret = posix_memalign(&input2, 4096, size_bytes);
        assert(ret == 0);
        ret = posix_memalign(&output, 4096, size_bytes*2);
        assert(ret == 0);

        ret = pread64(fd1, input1, size_bytes, 0);
        ret = pread64(fd2, input2, size_bytes, 0);

        uint64_t idx1 = 0;
        uint64_t idx2 = 0;
        T *ptr1, *ptr2, *ptr3;
        ptr1 = reinterpret_cast<T*>(input1);
        ptr2 = reinterpret_cast<T*>(input2);
        ptr3 = reinterpret_cast<T*>(output);
        uint64_t num_elements = size_bytes/ELEM_SIZE;
        auto start = std::chrono::high_resolution_clock::now();

        while (idx1 < num_elements && idx2 < num_elements) {
            bool b = (*ptr1 < *ptr2);
            T** ptr = b ? &ptr1: &ptr2;
            std::memcpy(reinterpret_cast<void*>(ptr3), reinterpret_cast<void*>(*ptr), ELEM_SIZE);
            *ptr += 1;
            ptr3 += 1;
            idx1 += b ? 1: 0;
            idx2 += b ? 0: 1;
        }
        if (idx1 < num_elements) {
            uint64_t bytes_to_copy = (num_elements - idx1) * ELEM_SIZE;
            std::memcpy(reinterpret_cast<void*>(ptr3), reinterpret_cast<void*>(ptr1), bytes_to_copy); 
        } 
        if (idx2 < num_elements) {
            uint64_t bytes_to_copy = (num_elements - idx2) * ELEM_SIZE;
            std::memcpy(reinterpret_cast<void*>(ptr3), reinterpret_cast<void*>(ptr2), bytes_to_copy);
        }

        auto end = std::chrono::high_resolution_clock::now();

        auto time_ms = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0f;
        float bw = (size_bytes * 2 * 1000) / (1024 * 1024 * 1024 * time_ms);
        printf("Total bytes written: %ld, Time: %f ms, BW: %f GBps\n", size_bytes * 2, time_ms, bw);
    }
};

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <key_size> <value_size> <intermediate_file_prefix>" << std::endl;
        return 1;
    }
    
    size_t key_size = std::stoull(argv[1]);
    size_t value_size = std::stoull(argv[2]);
    std::string intermediate_file_prefix = argv[3];
    
    // Instantiate MergeBenchmark based on key_size and value_size
    // Following the pattern from src/main.cpp
    if (key_size == 8 && value_size == 8) {
        MergeBenchmark<KeyValuePair<8, 8>> benchmark(intermediate_file_prefix);
        benchmark.run();
    } 
    else if (key_size == 8 && value_size == 24) {
        MergeBenchmark<KeyValuePair<8, 24>> benchmark(intermediate_file_prefix);
        benchmark.run();
    } 
    else if (key_size == 8 && value_size == 56) {
        MergeBenchmark<KeyValuePair<8, 56>> benchmark(intermediate_file_prefix);
        benchmark.run();
    } 
    else if (key_size == 8 && value_size == 120) {
        MergeBenchmark<KeyValuePair<8, 120>> benchmark(intermediate_file_prefix);
        benchmark.run();
    } 
    else if (key_size == 8 && value_size == 0) {
        MergeBenchmark<KeyValuePair<8, 0>> benchmark(intermediate_file_prefix);
        benchmark.run();
    } 
    else {
        std::cerr << "Error: Unsupported (key_size, value_size) combination: (" 
                  << key_size << ", " << value_size << ")" << std::endl;
        return 1;
    }
    
    return 0;
}