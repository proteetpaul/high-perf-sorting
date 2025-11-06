#include <string>
#include <key_index_pair.h>

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
        std::uintmax_t size_bytes = std::filesystem::file_size(path1);
        int fd1 = open(path1.c_str(), O_RDONLY | O_DIRECT);
        int fd2 = open(path2.c_str(), O_RDONLY | O_DIRECT);

        void *input1, *input2, *output;
        int ret = posix_memalign(&input1, 4096, size_bytes);
        assert(ret == 0);
        ret = posix_memalign(&input2, 4096, size_bytes);
        assert(ret == 0);
        ret = posix_memalign(&output, 4096, size_bytes);
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
        while (idx1 < num_elements && idx2 < num_elements) {
            bool b = (*ptr1 < *ptr2);
            T** ptr = b ? &ptr1: &ptr2;
            std::memcpy(ptr3, *ptr, ELEM_SIZE);
            *ptr += 1;
            idx1 += b ? 1: 0;
            idx2 += b ? 0: 1;
        }
        
    }
};