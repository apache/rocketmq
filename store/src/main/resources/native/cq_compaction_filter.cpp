/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Native compaction filter for ConsumeQueue entries.
 *
 * Subclass rocksdb::CompactionFilter directly, create instances in C++,
 * and pass the raw C++ pointer as a jlong to Java. Java's
 * AbstractCompactionFilter(nativeHandle) wraps it seamlessly.
 *
 * All rocksdb symbols are declared weak so they resolve at runtime to the
 * symbols already loaded by the JVM's ClassLoader.
 */

#include <atomic>
#include <cstdint>
#include <cstring>
#include <string>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/slice.h"

/* ------------------------------------------------------------------ */
/* Windows stub implementations                                       */
/*                                                                    */
/* On Linux/macOS, ELF/Mach-O shared libraries export all symbols by  */
/* default, so the shim resolves inherited virtual methods from       */
/* librocksdbjni at link time. On Windows, DLLs only export symbols   */
/* marked __declspec(dllexport) — rocksdbjni only exports JNI entry   */
/* points, not internal C++ class methods. We must provide stub       */
/* implementations for the Configurable/Customizable virtual methods  */
/* that appear in CompactionFilter's vtable. These stubs are never    */
/* called at runtime (RocksDB only invokes Filter() and Name() on     */
/* compaction filters), but the linker needs addresses for them.      */
/* ------------------------------------------------------------------ */

#ifdef _WIN32

#include "rocksdb/configurable.h"
#include "rocksdb/customizable.h"
#include <unordered_map>
#include <unordered_set>

namespace rocksdb {

struct ConfigOptions;
struct DBOptions;
struct ColumnFamilyOptions;
class OptionTypeInfo;

// --- Configurable virtual methods (defined in options/configurable.cc) ---

Status Configurable::GetOption(const ConfigOptions&, const std::string&,
                               std::string*) const {
    return Status();
}

bool Configurable::AreEquivalent(const ConfigOptions&, const Configurable*,
                                 std::string*) const {
    return true;
}

Status Configurable::PrepareOptions(const ConfigOptions&) {
    return Status();
}

Status Configurable::ValidateOptions(const DBOptions&,
                                     const ColumnFamilyOptions&) const {
    return Status();
}

const void* Configurable::GetOptionsPtr(const std::string&) const {
    return nullptr;
}

Status Configurable::ParseStringOptions(const ConfigOptions&,
                                        const std::string&) {
    return Status();
}

Status Configurable::ConfigureOptions(
    const ConfigOptions&,
    const std::unordered_map<std::string, std::string>&,
    std::unordered_map<std::string, std::string>*) {
    return Status();
}

Status Configurable::ParseOption(const ConfigOptions&, const OptionTypeInfo&,
                                 const std::string&, const std::string&,
                                 void*) {
    return Status();
}

bool Configurable::OptionsAreEqual(const ConfigOptions&, const OptionTypeInfo&,
                                   const std::string&, const void*,
                                   const void*, std::string*) const {
    return true;
}

std::string Configurable::SerializeOptions(const ConfigOptions&,
                                           const std::string&) const {
    return "";
}

std::string Configurable::GetOptionName(const std::string& name) const {
    return name;
}

// Non-virtual, but referenced by inline code paths
void Configurable::RegisterOptions(const std::string&, void*,
    const std::unordered_map<std::string, OptionTypeInfo>*) {}

Status Configurable::ConfigureFromMap(
    const ConfigOptions&,
    const std::unordered_map<std::string, std::string>&) {
    return Status();
}

Status Configurable::ConfigureFromMap(
    const ConfigOptions&,
    const std::unordered_map<std::string, std::string>&,
    std::unordered_map<std::string, std::string>*) {
    return Status();
}

Status Configurable::ConfigureOption(const ConfigOptions&, const std::string&,
                                     const std::string&) {
    return Status();
}

Status Configurable::ConfigureFromString(const ConfigOptions&,
                                         const std::string&) {
    return Status();
}

Status Configurable::GetOptionString(const ConfigOptions&,
                                     std::string*) const {
    return Status();
}

std::string Configurable::ToString(const ConfigOptions&,
                                   const std::string&) const {
    return "";
}

Status Configurable::GetOptionNames(const ConfigOptions&,
                                    std::unordered_set<std::string>*) const {
    return Status();
}

Status Configurable::GetOptionsMap(
    const std::string&, const std::string&, std::string*,
    std::unordered_map<std::string, std::string>*) {
    return Status();
}

// --- Customizable virtual/override methods (defined in options/customizable.cc) ---

Status Customizable::GetOption(const ConfigOptions&, const std::string&,
                               std::string*) const {
    return Status();
}

bool Customizable::AreEquivalent(const ConfigOptions&, const Configurable*,
                                 std::string*) const {
    return true;
}

std::string Customizable::GetOptionName(const std::string& name) const {
    return name;
}

std::string Customizable::SerializeOptions(const ConfigOptions&,
                                           const std::string&) const {
    return "";
}

std::string Customizable::GenerateIndividualId() const {
    return "stub";
}

Status Customizable::GetOptionsMap(
    const ConfigOptions&, const Customizable*, const std::string&,
    std::string*, std::unordered_map<std::string, std::string>*) {
    return Status();
}

Status Customizable::ConfigureNewObject(
    const ConfigOptions&, Customizable*,
    const std::unordered_map<std::string, std::string>&) {
    return Status();
}

// --- Status methods (defined in util/status.cc) ---

Status::Status(Code _code, SubCode _subcode, const Slice& msg,
               const Slice& msg2, Severity sev)
    : code_(_code), subcode_(_subcode), sev_(sev),
      retryable_(false), data_loss_(false), scope_(0) {}

std::unique_ptr<const char[]> Status::CopyState(const char* s) {
    if (s == nullptr) return nullptr;
    const size_t n = std::strlen(s) + 1;
    char* result = new char[n];
    std::memcpy(result, s, n);
    return std::unique_ptr<const char[]>(result);
}

std::string Status::ToString() const {
    return "OK";
}

}  // namespace rocksdb

#endif  // _WIN32

/* ------------------------------------------------------------------ */
/* Our concrete compaction filter                                     */
/* ------------------------------------------------------------------ */

class CqCompactionFilter : public rocksdb::CompactionFilter {
public:
    const char* Name() const override {
        return "ConsumeQueueCompactionFilter";
    }

    bool Filter(int /*level*/, const rocksdb::Slice& /*key*/,
                const rocksdb::Slice& existing_value, std::string* /*new_value*/,
                bool* /*value_changed*/) const override {
        static const int CQ_MIN_SIZE = 28;
        if (existing_value.size() < static_cast<size_t>(CQ_MIN_SIZE)) {
            return false;
        }
        const unsigned char* data =
            reinterpret_cast<const unsigned char*>(existing_value.data());
        int64_t phy_offset =
            (static_cast<int64_t>(data[0]) << 56) |
            (static_cast<int64_t>(data[1]) << 48) |
            (static_cast<int64_t>(data[2]) << 40) |
            (static_cast<int64_t>(data[3]) << 32) |
            (static_cast<int64_t>(data[4]) << 24) |
            (static_cast<int64_t>(data[5]) << 16) |
            (static_cast<int64_t>(data[6]) << 8) |
            (static_cast<int64_t>(data[7]));

        int64_t min_offset = min_phy_offset_.load(std::memory_order_relaxed);
        return phy_offset < min_offset;
    }

    void SetMinPhyOffset(int64_t offset) {
        min_phy_offset_.store(offset, std::memory_order_relaxed);
    }

private:
    std::atomic<int64_t> min_phy_offset_{0};
};

/* ------------------------------------------------------------------ */
/* JNI bindings                                                       */
/* ------------------------------------------------------------------ */

#include <jni.h>

extern "C" {

JNIEXPORT jlong JNICALL
Java_org_apache_rocketmq_store_rocksdb_CqCompactionFilterJni_createNativeFilter0(
    JNIEnv* env, jclass clazz) {
    CqCompactionFilter* filter = new CqCompactionFilter();
    return reinterpret_cast<jlong>(filter);
}

JNIEXPORT void JNICALL
Java_org_apache_rocketmq_store_rocksdb_CqCompactionFilterJni_setMinPhyOffset0(
    JNIEnv* env, jclass clazz, jlong filterPtr, jlong minPhyOffset) {
    CqCompactionFilter* filter = reinterpret_cast<CqCompactionFilter*>(filterPtr);
    filter->SetMinPhyOffset(minPhyOffset);
}

} // extern "C"
