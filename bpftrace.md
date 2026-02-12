# bpftrace: Per-block device read/write byte totals

```bash
sudo bpftrace -e '
tracepoint:block:block_rq_complete
{
  $bytes = args->nr_sector * 512;
  /* Cast the string to a char pointer and dereference to get the first byte */
  $c = *(uint8 *)args->rwbs;
  $dev = args->dev;

  @read[$dev]  += ($c == 82 ? $bytes : 0);   /* 82 is "R" */
  @write[$dev] += ($c == 87 ? $bytes : 0);   /* 87 is "W" */
  @total[$dev] += $bytes;
  @seen[$dev] = 1;
}
'
```

# bpftrace: Block IO bandwidth tracking at 10ms intervals

```bash
sudo bpftrace -e '
tracepoint:block:block_rq_complete
{
  $bytes = args->nr_sector * 512;
  /* Cast the string to a char pointer and dereference to get the first byte */
  $c = *(uint8 *)args->rwbs;
  
  /* Accumulate bytes for reads and writes */
  @read_bytes  += ($c == 82 ? $bytes : 0);
  @write_bytes += ($c == 87 ? $bytes : 0);
}

interval:ms:10
{
  /* Calculate bandwidth in MB/s using integer arithmetic */
  /* bytes in 10ms -> bytes_per_second = bytes * 100 */
  /* MB_per_second = (bytes * 100) / 1048576 */
  $read_bps = @read_bytes * 100;        /* bytes per second */
  $write_bps = @write_bytes * 100;
  $read_mbps = $read_bps / 1048576;     /* MB per second (integer division) */
  $write_mbps = $write_bps / 1048576;
  
  /* Time in seconds (elapsed is in nanoseconds) */
  $time_sec = elapsed / 1000000000;
  $time_ms_remainder = elapsed % 1000000000;
  $time_ms = $time_ms_remainder / 1000000;
  
  printf("%d.%03d: Read: %d MB/s, Write: %d MB/s\n", 
         $time_sec, $time_ms, $read_mbps, $write_mbps);
  
  /* Reset counters for next interval */
  @read_bytes = 0;
  @write_bytes = 0;
}

END
{
  printf("\nTracing complete.\n");
}
'
```
