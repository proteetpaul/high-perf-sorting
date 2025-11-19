sudo apt -y install libtbb-dev cmake python3-numpy python3-pandas libboost-all-dev

echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
export PCM_NO_MSR=1
export PCM_KEEP_NMI_WATCHDOG=1

wget https://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz

mkdir -p build && cd build
cmake -DIPS4O_DISABLE_PARALLEL=ON ..
cmake --build . --parallel

