#include <iostream>
#include <stdexcept>
#include <string>
#include <cstdlib>
#include <sstream>
#include <iomanip>
#include <pthread.h>
#include <sched.h>

#include "key_value_pair.h"
#include "sorter.h"
#include "config.h"

struct ParsedArgs {
    std::string working_dir;
    size_t input_bytes;
    size_t key_size;
    size_t value_size;
    size_t memory_size;
    uint32_t num_threads;
    bool separate_values;
};

size_t parseSizeString(const std::string& sizeStr) {
    if (sizeStr.empty()) {
        throw std::invalid_argument("Empty size string");
    }
    
    // Find the last character to determine the unit
    size_t lastChar = sizeStr.length() - 1;
    char unit = std::tolower(sizeStr[lastChar]);
    
    // Extract the numeric part
    std::string numericPart;
    if (std::isdigit(unit)) {
        // No unit specified, assume bytes
        numericPart = sizeStr;
        unit = 'b';
    } else {
        // Unit specified, extract numeric part
        numericPart = sizeStr.substr(0, lastChar);
    }
    
    if (numericPart.empty()) {
        throw std::invalid_argument("No numeric value found in size string");
    }
    
    size_t multiplier = 1;
    switch (unit) {
        case 'b':
            multiplier = 1;
            break;
        case 'k':
            multiplier = 1024;
            break;
        case 'm':
            multiplier = 1024 * 1024;
            break;
        case 'g':
            multiplier = 1024 * 1024 * 1024;
            break;
        default:
            throw std::invalid_argument("Unknown unit: " + std::string(1, unit) + ". Use B, K, M, or G");
    }
    
    size_t value = std::stoull(numericPart);
    return value * multiplier;
}

std::string formatFileSize(size_t bytes) {
    const char* units[] = {"B", "K", "M", "G", "T"};
    int unit = 0;
    double size = static_cast<double>(bytes);
    
    while (size >= 1024.0 && unit < 4) {
        size /= 1024.0;
        unit++;
    }
    
    // Format with appropriate precision
    std::ostringstream oss;
    if (unit == 0) {
        oss << static_cast<int>(size);
    } else if (size >= 100.0) {
        oss << static_cast<int>(size);
    } else if (size >= 10.0) {
        oss << static_cast<int>(size);
    } else {
        oss << static_cast<int>(size);
    }
    
    oss << units[unit];
    return oss.str();
}

void printHelp(const char* program_name) {
    std::cout << "Usage: " << program_name << " [options]\n";
    std::cout << "Options:\n";
    std::cout << "  --working-dir <dir>              Working directory (default: .)\n";
    std::cout << "  --file-size <size>               Input file size with unit (default: 1M)\n";
    std::cout << "  --key-size <size>                Key size in bytes (default: 10)\n";
    std::cout << "  --value-size <size>              Value size in bytes (default: 90)\n";
    std::cout << "  --memory-size <size>             Memory size with unit (default: 100M)\n";
    std::cout << "  --read-chunk-size <size>         Read chunk size with unit (default: 100M)\n";
    std::cout << "  --num-threads <count>            Number of threads for parallel sorting (default: 1)\n";
    std::cout << "  --help, -h                       Show this help message\n";
    std::cout << "\nSize units: B (bytes), K (KB), M (MB), G (GB)\n";
    std::cout << "Examples: 1M, 512K, 2G, 1024B\n";
}

int parseArguments(int argc, char* argv[], ParsedArgs& args) {
    // Set default values
    args.working_dir = ".";
    args.input_bytes = parseSizeString("1M");  // 1MB default
    args.key_size = 10;
    args.value_size = 90;
    args.memory_size = parseSizeString("100M");  // 100MB default
    args.num_threads = 1;  // 1 thread default
    args.separate_values = false;
    
    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        try {
            if (arg == "--working-dir" && i + 1 < argc) {
                args.working_dir = argv[++i];
            }
            else if (arg == "--file-size" && i + 1 < argc) {
                args.input_bytes = parseSizeString(argv[++i]);
            }
            else if (arg == "--key-size" && i + 1 < argc) {
                args.key_size = std::stoull(argv[++i]);
            }
            else if (arg == "--value-size" && i + 1 < argc) {
                args.value_size = std::stoull(argv[++i]);
            }
            else if (arg == "--memory-size" && i + 1 < argc) {
                args.memory_size = parseSizeString(argv[++i]);
            }
            else if (arg == "--num-threads" && i + 1 < argc) {
                args.num_threads = std::stoul(argv[++i]);
            }
            else if (arg == "--help" || arg == "-h") {
                printHelp(argv[0]);
                return 0;
            }
            else if (arg == "--separate-values") {
                args.separate_values = true;
            }
            else {
                std::cerr << "Unknown argument: " << arg << std::endl;
                std::cerr << "Use --help for usage information." << std::endl;
                return 1;
            }
        }
        catch (const std::exception& e) {
            std::cerr << "Error parsing argument '" << arg << "': " << e.what() << std::endl;
            return 1;
        }
    }
    
    return 2;  // Success, continue execution
}

// Pin current thread to cpu 0
void pin_current_thread() {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset);
    pthread_t current_thread = pthread_self();
    pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

int main(int argc, char* argv[]) {
    // pin_current_thread();
    ParsedArgs args;
    int parse_result = parseArguments(argc, argv, args);
    
    if (parse_result == 0) {
        return 0;  // Help was printed
    }
    else if (parse_result == 1) {
        return 1;  // Error occurred
    }
    
    // Create Config object and set values
    Config config;
    config.num_threads = args.num_threads;
    config.run_size_bytes = args.memory_size;
    config.file_size_bytes = args.input_bytes;
    // Create human-friendly file names with size information
    std::string file_size_str = formatFileSize(args.input_bytes);
    config.input_file = args.working_dir + "/input-" + file_size_str + ".dat";
    config.output_file = args.working_dir + "/output-" + file_size_str + ".dat";
    config.intermediate_file_prefix = args.working_dir + "/intermediate-" + file_size_str;
    config.separate_values = args.separate_values;
    
    // Print parsed arguments for verification
    std::cout << "Config object values:\n";
    std::cout << "  Number of threads: " << config.num_threads << std::endl;
    std::cout << "  Run size bytes: " << config.run_size_bytes << std::endl;
    std::cout << "  File size bytes: " << config.file_size_bytes << std::endl;
    std::cout << "  Input file: " << config.input_file << std::endl;
    std::cout << "  Output file: " << config.output_file << std::endl;
    std::cout << "  Intermediate file: " << config.intermediate_file_prefix << std::endl;
    std::cout << "  Number of runs: " << config.num_runs() << std::endl;
    
    // TODO: Use config object with your sorter implementation
    if (args.key_size == 8 && args.value_size == 8) {
        std::cout << "calling sort...\n";
        Sorter<KeyValuePair<8, 8>> sorter(std::move(config));
        sorter.sort();
        sorter.print_timing_stats();
    } 
    else if (args.key_size == 8 && args.value_size == 24) {
        Sorter<KeyValuePair<8, 24>> sorter(std::move(config));
        sorter.sort();
        sorter.print_timing_stats();
    } else if (args.key_size == 8 && args.value_size == 56) {
        Sorter<KeyValuePair<8, 56>> sorter(std::move(config));
        sorter.sort();
        sorter.print_timing_stats();
    } else if (args.key_size == 8 && args.value_size == 120) {
        Sorter<KeyValuePair<8, 120>> sorter(std::move(config));
        sorter.sort();
        sorter.print_timing_stats();
    } else {
        throw std::runtime_error("Not all (key size, value size) combinations allowed");
    }
    return 0;
}