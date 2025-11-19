#pragma once

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>

struct read_format {
    uint64_t value;
    uint64_t id;
};

static int perf_event_open(struct perf_event_attr *hw_event, pid_t pid, int cpu, int group_fd, unsigned long flags){
    int fd;
    fd = syscall(SYS_perf_event_open, hw_event, pid, cpu, group_fd, flags);
    if (fd == -1) {
        fprintf(stderr, "Error creating event\n");
        exit(EXIT_FAILURE);
    }
  
    return fd;
}

void configure_event(struct perf_event_attr *pe, uint64_t config){
    memset(pe, 0, sizeof(struct perf_event_attr));
    pe->type = PERF_TYPE_HARDWARE;
    pe->size = sizeof(struct perf_event_attr);
    pe->config = config;
    pe->read_format = PERF_FORMAT_ID;
    pe->disabled = 1;
    pe->exclude_kernel = 1;
    pe->exclude_hv = 1;
}

int init_perf_counter(uint64_t config) {
    perf_event_attr pe;
    configure_event(&pe, config);
    int fd = perf_event_open(&pe, 0, -1, -1, 0);
    ioctl(fd, PERF_EVENT_IOC_RESET, 0);
    ioctl(fd, PERF_EVENT_IOC_ENABLE, 0);
    return fd;
}

int read_perf_counter(int fd) {
    read_format result;
    ioctl(fd, PERF_EVENT_IOC_DISABLE, 0);
    int ret = read(fd, &result, sizeof(struct read_format));
    assert(ret > 0);
    return result.value;
}