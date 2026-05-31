# Native RocksDB ConsumeQueue Compaction Filter

## Background

RocketMQ previously depended on a custom-forked RocksDB Java binding published as `org.apache.rocketmq:rocketmq-rocksdb:1.0.6`. This fork was maintained in the `apache/rocketmq-externals` repository and was essentially a republished copy of `org.rocksdb:rocksdbjni` with exactly **one** additional class:

- `org.rocksdb.RemoveConsumeQueueCompactionFilter` — a RocksDB compaction filter that removes stale consume queue entries during compaction. Its C++ implementation and JNI glue lived in the forked C++ source tree under `utilities/compaction_filters/remove_consumequeue_compactionfilter.*` and `java/rocksjni/remove_consumequeue_compactionfilterjni.cc`.

All other RocketMQ subsystems using RocksDB (Pop consumption state, config storage, index storage, timer storage, transaction half-message storage) used only standard RocksDB Java APIs and had no dependency on the fork's custom code.

## Problem

Maintaining a fork of RocksDB's Java bindings has several costs:

1. **Upgrade friction** — every RocksDB upstream release requires rebuilding the entire fork to pick up the new native library and Java API
2. **Native build complexity** — the fork bundles a full C++ build pipeline for multiple platforms (Linux glibc/musl, macOS, Windows)
3. **Dependency duplication** — the `rocksdb/` module in the RocketMQ source tree duplicates ~190 classes that are identical to upstream `rocksdbjni`
4. **License ambiguity** — the fork republishes Facebook's RocksDB code under the Apache group

## Solution

Replace `rocketmq-rocksdb` with the official `org.rocksdb:rocksdbjni:8.4.4` and move the single custom compaction filter into a standalone native shim.

### Why a native shim is needed

The official `rocksdbjni` provides a `ColumnFamilyOptions.setCompactionFilter(AbstractCompactionFilter)` method, but its `AbstractCompactionFilter` Java class requires a native handle (raw `rocksdb::CompactionFilter*` pointer) passed to its constructor. The Java `filter()` method callback goes through a C++ trampoline that RocksDB's JNI layer manages internally — you can only subclass it from within the same JNI compilation unit.

To implement a custom compaction filter outside the `rocksdbjni` build, we create a standalone C++ shared library that:
- Directly subclasses `rocksdb::CompactionFilter` in C++
- Exposes JNI methods to create filter instances and update the `minPhyOffset` threshold
- Returns the raw `CompactionFilter*` pointer as a `jlong` to Java

### Architecture

```
┌──────────────────────────────────────────────────────┐
│  ConsumeQueueRocksDBStorage (Java)                   │
│  - CqCompactionFilterJni.createAndSetFilter(cqCfOpts)│
│  - CqCompactionFilterJni.setMinPhyOffset(offset)     │
└──────────────────┬───────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────┐
│  CqCompactionFilterJni.java                          │
│  - Extracts libcq_compaction_filter.so to temp dir   │
│  - Uses NativeCqCompactionFilter wrapper with        │
│    disOwnNativeHandle() + public setCompactionFilter │
│  - Calls native createNativeFilter0() → raw pointer  │
└──────────────────┬───────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────┐
│  libcq_compaction_filter.so (native shim)            │
│                                                      │
│  class CqCompactionFilter                            │
│    : public rocksdb::CompactionFilter { ... }        │
│                                                      │
│  JNI: createNativeFilter0() → new CqCompactionFilter │
│  JNI: setMinPhyOffset0(ptr, offset)                  │
│                                                      │
│  Self-contained: stub vtable methods inline,         │
│  NO DT_NEEDED on librocksdbjni                       │
└──────────────────────────────────────────────────────┘
```

### Key design decisions

**1. Self-contained C++ shim with stub vtable methods**

The shim directly subclasses `rocksdb::CompactionFilter` in C++ and is compiled with matching ABI flags (`-fno-rtti -D_GLIBCXX_USE_CXX11_ABI=0`). All inherited virtual methods from `Configurable` and `Customizable` are provided as inline stub implementations, making the shim fully self-contained with **no runtime dependency on librocksdbjni**. This avoids the JVM crash caused by `dlopen` loading a second copy of rocksdbjni (which has no SONAME — different temp paths produce different inodes, so `dlopen` treats them as separate libraries, duplicating global state).

**2. Raw pointer as jlong, wrapped with disOwnNativeHandle()**

The native shim creates `new CqCompactionFilter()` and returns the raw C++ pointer as a `jlong`. A thin Java wrapper `NativeCqCompactionFilter extends AbstractCompactionFilter<Slice>` passes this pointer to the protected `AbstractCompactionFilter(long)` constructor, then calls `disOwnNativeHandle()` so that `close()` does not free the native memory. This is critical because `AbstractRocksDBStorage.shutdown()` closes `ColumnFamilyOptions` (step 2) before closing the DB (step 4) — without `disOwnNativeHandle()`, background compaction threads would access a freed filter. The filter is then set via the public `ColumnFamilyOptions.setCompactionFilter()` API, avoiding reflection and ensuring JDK 17+ compatibility.

**3. Simple temp file extraction**

At runtime, `CqCompactionFilterJni` extracts the shim library to a temp file and calls `System.load()`. Since the shim is self-contained (no `DT_NEEDED` on rocksdbjni), it can be loaded from any directory without worrying about library path resolution. The rocksdbjni native library is loaded separately by `AbstractRocksDBStorage` via `RocksDB.loadLibrary()` — the two libraries are independent at the dynamic linker level.

**4. Thread-safe minPhyOffset with std::atomic**

The `CqCompactionFilter` uses `std::atomic<int64_t>` with `memory_order_relaxed` for `min_phy_offset_`. This is sufficient because there is a single writer (Java main thread via JNI) and one reader (compaction background thread), and eventual consistency is acceptable — a slightly stale threshold only means a few extra entries survive one compaction cycle. This replaces the earlier `pthread_mutex` approach, eliminating per-entry lock/unlock overhead during full compaction over hundreds of millions of entries.

## Changed files

| File | Change |
|------|--------|
| `pom.xml` | `rocksdb.version` → `rocksdbjni.version=8.4.4`; dependency changed to `org.rocksdb:rocksdbjni` |
| `common/pom.xml` | `rocketmq-rocksdb` → `org.rocksdb:rocksdbjni` |
| `common/.../config/AbstractRocksDBStorage.java` | `manualCompactionDefaultCfRange` enhanced with `estimateNumKeys` logging (before/after key count, elapsed time, reduction ratio); `manualCompaction` removed unused `minPhyOffset` parameter |
| `store/.../rocksdb/ConsumeQueueCompactionFilterFactory.java` | **Deleted** — replaced by native shim |
| `store/.../rocksdb/ConsumeQueueRocksDBStorage.java` | Use `CqCompactionFilterJni.createAndSetFilter()` instead of `CompactionFilterFactory`; added `triggerCompactionSync()` and `countEntries()` helpers |
| `store/.../rocksdb/RocksDBOptionsFactory.java` | Remove `setCompactionFilterFactory()` call from `createCQCFOptions()` |
| `store/.../rocksdb/CqCompactionFilterJni.java` | **Rewritten** — uses raw JNI pointer + `NativeCqCompactionFilter` wrapper via public API; added platform-aware library name detection (macOS `.dylib` / Linux `.so` / Windows `.dll`) |
| `store/.../rocksdb/NativeCqCompactionFilter.java` | **New** — thin `AbstractCompactionFilter<Slice>` wrapper with `disOwnNativeHandle()` |
| `store/.../resources/native/cq_compaction_filter.cpp` | **Rewritten** — direct C++ subclassing, explicit linking, `std::atomic` for thread safety |
| `store/.../resources/native/libcq_compaction_filter.so` | **New** — pre-compiled native library (Linux x86_64) |
| `store/.../resources/native/libcq_compaction_filter.dylib` | **New** — pre-compiled native library (macOS arm64) |
| `store/.../resources/native/cq_compaction_filter.dll` | **New** — pre-compiled native library (Windows x86_64, MSVC v14.29) |
| `store/.../rocksdb/CqCompactionFilterJniTest.java` | **New** — integration test for compaction filter |

## Building the native shim

Prerequisites: `g++` / `clang++`, RocksDB C++ headers matching `rocksdbjni` version (8.4.4), JNI headers from your JDK.

### Linux (x86_64)

```bash
# 1. Download matching RocksDB headers (only headers needed — no linking against rocksdbjni)
wget https://github.com/facebook/rocksdb/archive/refs/tags/v8.4.4.tar.gz
tar xzf v8.4.4.tar.gz rocksdb-8.4.4/include --strip-components=1

# 2. Compile the self-contained shim
export JAVA_HOME=/usr/lib/jvm/java-8   # or your JDK path
g++ -shared -fPIC -O2 -std=c++17 -fno-rtti -D_GLIBCXX_USE_CXX11_ABI=0 \
    -I./include \
    -I${JAVA_HOME}/include \
    -I${JAVA_HOME}/include/linux \
    -o libcq_compaction_filter.so \
    store/src/main/resources/native/cq_compaction_filter.cpp

# 3. Verify no dependency on rocksdbjni
objdump -p libcq_compaction_filter.so | grep NEEDED
# Should show ONLY: libstdc++, libm, libgcc_s, libc — NOT librocksdbjni
nm -D libcq_compaction_filter.so | grep " U " | grep rocksdb
# Should be empty (no undefined rocksdb symbols)

# 4. Replace the pre-built .so
cp libcq_compaction_filter.so store/src/main/resources/native/
```

### macOS (arm64 / x86_64)

```bash
# 1. Download matching RocksDB headers
#    Use ghproxy.net mirror if github.com is blocked:
curl -sL "https://ghproxy.net/https://github.com/facebook/rocksdb/archive/refs/tags/v8.4.4.tar.gz" -o /tmp/rocksdb-8.4.4.tar.gz
tar xzf /tmp/rocksdb-8.4.4.tar.gz -C /tmp rocksdb-8.4.4/include --strip-components=1
# Or use a local RocksDB checkout if available:
# ROCKSDB_INCLUDE=/path/to/rocksdb/include

# 2. Compile the self-contained shim (no linking against rocksdbjni needed)
export JAVA_HOME=$(/usr/libexec/java_home)
ROCKSDB_INCLUDE=${ROCKSDB_INCLUDE:-./include}  # adjust to your headers location
clang++ -shared -fPIC -O2 -std=c++17 -fno-rtti \
    -I"$ROCKSDB_INCLUDE" \
    -I${JAVA_HOME}/include \
    -I${JAVA_HOME}/include/darwin \
    -o libcq_compaction_filter.dylib \
    store/src/main/resources/native/cq_compaction_filter.cpp

# 3. Verify no dependency on rocksdbjni
otool -L libcq_compaction_filter.dylib
# Should show ONLY system libs (libc++, libSystem) — NOT librocksdbjni

# 4. Place the output
cp libcq_compaction_filter.dylib store/src/main/resources/native/
```

### Windows (x86_64)

**⚠ Must use MSVC — MinGW is NOT compatible** (different vtable layout, name mangling, exception handling).

```powershell
# 1. Set up environment (run vcvarsall.bat first, or use the VS Dev Command Prompt)
set "VCTools=C:\Program Files\Microsoft Visual Studio\2022\BuildTools\VC\Tools\MSVC\14.29.30133"
set "SDK=C:\Program Files (x86)\Windows Kits\10"
set "JAVA_HOME=C:\path\to\jdk8"

# 2. Extract RocksDB headers
curl -LO https://github.com/facebook/rocksdb/archive/refs/tags/v8.4.4.tar.gz
tar xzf v8.4.4.tar.gz rocksdb-8.4.4/include --strip-components=1

# 3. Compile with MSVC cl.exe (no linking against rocksdbjni needed)
cl.exe /LD /O2 /std:c++17 /GR- /EHsc /utf-8 ^
   /I"%JAVA_HOME%\include" ^
   /I"%JAVA_HOME%\include\win32" ^
   /I"rocksdb-8.4.4\include" ^
   /I"%VCTools%\include" ^
   /I"%SDK%\Include\10.0.19041.0\ucrt" ^
   /I"%SDK%\Include\10.0.19041.0\shared" ^
   /I"%SDK%\Include\10.0.19041.0\um" ^
   /Fecq_compaction_filter.dll ^
   store\src\main\resources\native\cq_compaction_filter.cpp ^
   /link /MACHINE:X64 ^
   /LIBPATH:"%VCTools%\lib\x64" ^
   /LIBPATH:"%SDK%\Lib\10.0.19041.0\ucrt\x64" ^
   /LIBPATH:"%SDK%\Lib\10.0.19041.0\um\x64"

# 4. Verify exports
dumpbin /exports cq_compaction_filter.dll
# Should show: Java_org_apache_rocketmq_store_rocksdb_CqCompactionFilterJni_createNativeFilter0
#             Java_org_apache_rocketmq_store_rocksdb_CqCompactionFilterJni_setMinPhyOffset0

# 5. Place the output
copy cq_compaction_filter.dll store\src\main\resources\native\
```

> **Note on Git Bash / MSYS2**: Use `MSYS2_ARG_CONV_EXCL='*'` to prevent path corruption of `/LD`, `/O2` etc.

**Option B: Run on WSL (recommended for development)**

```bash
# In WSL (Ubuntu)
mvn test -pl store -Dtest=CqCompactionFilterJniTest -Djacoco.skip=true
```

## Platform support

`CqCompactionFilterJni.java` automatically detects the OS and architecture at runtime, selecting the correct library name and extension.

| Platform | Library name | Architecture | Status |
|----------|-------------|--------------|--------|
| Linux (glibc) | `libcq_compaction_filter.so` | x86_64 | Pre-built |
| Linux (glibc) | `libcq_compaction_filter_aarch64.so` | aarch64 | Pre-built |
| macOS | `libcq_compaction_filter.dylib` | arm64 | Pre-built |
| macOS | `libcq_compaction_filter.dylib` | x86_64 | Requires rebuild |
| Windows | `cq_compaction_filter.dll` | x86_64 | Pre-built |

## Limitations

1. **Jacoco incompatibility** — The jacoco Java agent can cause native crashes when combined with dynamically loaded native libraries. Unit tests should be run with `-Djacoco.skip=true` when testing RocksDB functionality.

2. **Global singleton filter** — `CqCompactionFilterJni` stores the native filter pointer in a static `AtomicLong NATIVE_FILTER_PTR`. Only one filter instance is tracked globally per JVM. If multiple `ConsumeQueueRocksDBStorage` instances exist (e.g., in tests or multi-Broker processes), `setMinPhyOffset()` always updates the last-created filter. Earlier instances lose their threshold updates silently.

3. **C++17 required** — The C++ source uses `std::atomic<int64_t>` which requires a C++17-capable compiler. All modern compilers (GCC 7+, Clang 5+, MSVC 2017+) support this.

4. **Windows requires MSVC** — MinGW produces incompatible C++ binaries (different vtable layout). Must use MSVC for the Windows build.

5. **Stub vtable methods must match RocksDB version** — The shim provides stub implementations of `Configurable`/`Customizable`/`Status` methods to fill the vtable. If the upstream RocksDB header adds new virtual methods to these classes in a future version, the stubs must be updated accordingly.
