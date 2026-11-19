# NameServer topic route response cache

NameServer can reuse encoded topic route responses between route changes. Enable it
in the NameServer configuration before startup, for example:

```properties
routeInfoCacheMaxBytes=67108864
```

The default is `0`, which disables caching. This option requires a restart. A
positive value bounds the retained response bytes, topic names, ordering values
and a conservative fixed weight per entry; it is not a hard limit on JVM heap or
temporary allocations made by concurrent requests. Oversized responses are served
without caching. Only queried topics occupy cache space.

The cache uses FIFO eviction. Hits do not update shared eviction bookkeeping or
allocate access records. Entries are refreshed on lookup after five minutes;
unused entries can remain resident until capacity eviction, within the weight
bound. Standard JSON and legacy encoding variants are filled independently and
counted together. Response bodies are shared read-only; response hooks must
replace the body when transforming it. The built-in zone hook already does so.

Effective route changes (topic/queue updates, broker addresses, zones,
acting-master metadata, filter servers, permissions, static topic mappings and
unregistration) advance a global revision. Unchanged registrations and liveness
heartbeats preserve cached responses. A request overlapping a route mutation may
return the earlier snapshot; subsequent requests observing the completed mutation
cannot reuse that snapshot. JSON serialization runs outside the route lock.

The ordering configuration is read from KV configuration on every query. Its
current value and the acting-master switch are checked when looking up a cached
response. No request-specific zone-filtered response is stored in the cache.

This is intended for repeated queries of relatively stable routes. Changes to an
unrelated topic also invalidate existing entries, so route churn lowers the hit
rate. A cache smaller than the queried working set can churn and cost more than
uncached lookup. Size it using realistic route sizes and measure both query and
registration workloads before enabling it. Registration never walks or rebuilds
the cache.

`TopicRouteCacheBenchmark` compares the disabled and enabled paths, standalone
registration, and a group with four query threads and one registration thread.
The benchmark changes decoded registration versions while retaining equivalent
routes, exercising per-topic registration comparison without intentionally
invalidating the query working set. Examples using a test classpath containing
`namesrv/target/test-classes`, `namesrv/target/classes` and test dependencies:

```text
org.openjdk.jmh.Main 'TopicRouteCacheBenchmark.query$' -p topics=1,10000 -p brokers=8 -p cacheBytes=0,67108864 -t 1 -prof gc
org.openjdk.jmh.Main 'TopicRouteCacheBenchmark.registration$' -p topics=10000 -p brokers=8 -p cacheBytes=0,67108864 -t 1 -prof gc
org.openjdk.jmh.Main 'TopicRouteCacheBenchmark.mixed$' -p topics=10000 -p brokers=8 -p cacheBytes=0,67108864 -t 5 -prof gc
```

These measure Java method calls and allocation, not end-to-end network latency or
production throughput. Retain raw fork results, assess variance, and include route
churn, undersized caches and large registration payloads in deployment testing.

## Local comparison (2026-10-21)

Base: `57c2dc04f78cc7e73e6b8c9436ee0fff0d0f46c3`. Windows x86_64,
Ryzen 9 7945HX (16 cores / 32 logical processors), Microsoft OpenJDK 17.0.17,
JMH 1.19, fixed 512 MiB heap, 10,000 queried topics and 8 brokers. Two forks,
three one-second warmups and three one-second measurements per fork; `-prof gc`.
The enabled cache is 64 MiB and contains the query working set.

| Method, one thread | Cache | Average ns/op | JMH 99.9% error | Allocated B/op |
| --- | --- | ---: | ---: | ---: |
| Query | disabled | 1,982.5 | 136.8 | 5,816.5 |
| Query | enabled | 32.8 | 8.9 | < 0.001 |
| Registration | unmodified upstream | 1,049,852.9 | 697,579.0 | 320,540.3 |
| Registration | disabled | 828,471.8 | 319,719.4 | 320,497.4 |
| Registration | enabled | 689,833.4 | 316,458.7 | 320,498.9 |

The registration uncertainty intervals overlap substantially. These samples do
not establish a registration speedup or rule out regressions under route churn
or larger deployments.

A separate group run (four query threads, one registration thread; two warmups,
three measurements and two forks) recorded the following subgroup means:

| Group method | Cache disabled ns/op | Cache enabled ns/op |
| --- | ---: | ---: |
| Query during registration | 58,184,934.2 | 41.1 |
| Concurrent registration | 902,116.3 | 806,096.2 |

The uncached read result is dominated by contention with continuous registration;
it is not a steady-state read latency claim. The group-wide GC allocation result
mixes registration and reads and must not be presented as query allocation.

Query iteration averages, in ns/op, grouped by fork:

```text
disabled: [1978.193, 1921.500, 1931.746], [2006.841, 2048.228, 2008.461]
enabled:  [29.789, 29.622, 30.321], [35.410, 35.914, 35.737]
```
