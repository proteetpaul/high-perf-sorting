The goal of this repository is to implement a high performance external merge sort framework using SSDs. Important files:
- run.sh: Runner script which calls src/main.cpp, adds profiling
- src/main.cpp: Entry point to running the actual sorting benchmark
- src/sorter.h: Contains the central Sorter class, which is a template. It uses in-place sample sort (ips4o) for generating sorted runs, and origami in order to merge these runs. It also allows separation of values from keys
- key_value_pair.h: Contains a templatized definition of a key-value pair
- merge.h: Uses Origami merge sort framework, or more specifically, mtree, to concurrently merge the sorted runs. Origami might have issues with handling unaligned values, so for now we cut off the beginning and end to ensure 64-byte alignment of the input.
- partition.h: Takes a set of sorted input runs and partitions each of them to create smaller runs that can be independently merged without synchronization.