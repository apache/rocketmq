# The System Configuration (RocketMQ 5.x)

This section focuses on the configuration of the system (JVM/OS) for **RocketMQ 5.x**.

## **1 JVM Options** ##

The latest released version of JDK 1.8 is recommended. Set the same Xms and Xmx value to prevent the JVM from resizing the heap for better performance. A simple JVM configuration is as follows:

    -server -Xms8g -Xmx8g -Xmn4g

Direct ByteBuffer memory size setting. Full GC will be triggered when the Direct ByteBuffer up to the specified size:

    -XX:MaxDirectMemorySize=15g

If you don’t care about the boot time of RocketMQ broker, pre-touch the Java heap to make sure that every page will be allocated during JVM initialization is a better choice. Those who don’t care about the boot time can enable it:
    
    -XX:+AlwaysPreTouch

Disable biased locking to potentially reduce JVM pauses:

    -XX:-UseBiasedLocking

As for garbage collection, the G1 collector with JDK 1.8 is recommended:

    -XX:+UseG1GC -XX:G1HeapRegionSize=16m
    -XX:G1ReservePercent=25
    -XX:InitiatingHeapOccupancyPercent=30

These GC options may seem a bit aggressive, but they have been proven to provide good performance in our production environment.

Do not set a too small value for -XX:MaxGCPauseMillis, as it may cause the JVM to use a small young generation, leading to frequent minor GCs. Using a rolling GC log file is recommended:
    
    -XX:+UseGCLogFileRotation
    -XX:NumberOfGCLogFiles=5
    -XX:GCLogFileSize=30m
    
If writing GC logs to disk increases broker latency, consider redirecting the GC log file to a memory file system:
    
    -Xloggc:/dev/shm/mq_gc_%p.log123

## **2 Linux Kernel Parameters** ##

There is an `os.sh` script in the `bin` folder that lists several kernel parameters that can be used for production with minor modifications. Below parameters require attention. For more details, refer to the documentation for `/proc/sys/vm/*`.

- **vm.extra_free_kbytes**: Tells the VM to keep extra free memory between the threshold where background reclaim (kswapd) kicks in and the threshold where direct reclaim (by allocating processes) kicks in. RocketMQ uses this parameter to avoid high latency in memory allocation. (Kernel version-specific)

- **vm.min_free_kbytes**: If set lower than 1024KB, the system may become subtly broken and prone to deadlocks under high loads.

- **vm.max_map_count**: Limits the maximum number of memory map areas a process may have. RocketMQ uses `mmap` to load `CommitLog` and `ConsumeQueue`, so setting a higher value is recommended.

- **vm.swappiness**: Defines how aggressively the kernel swaps memory pages. Higher values increase aggressiveness, while lower values decrease the amount of swap. A value of **10** is recommended to avoid swap latency.

- **File descriptor limits**: RocketMQ requires open file descriptors for files (`CommitLog` and `ConsumeQueue`) and network connections. It is recommended to set this to **655350**.

- **Disk scheduler**: The **deadline I/O scheduler** is recommended for RocketMQ as it attempts to provide a guaranteed latency for requests.

---
