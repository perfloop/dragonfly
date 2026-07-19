// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
// A measurement-only companion for perfloop_pubsub.py.  It is linked into an
// instrumented Dragonfly binary, never into a production target.  GCC calls
// __cyg_profile_func_enter at every function entry; this file counts only the
// callback-invoker addresses selected from the just-built binary by the driver.

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

namespace {

constexpr size_t kMaxTargets = 32;

std::atomic<uint64_t> callback_entries{0};
uintptr_t targets[kMaxTargets] = {};
size_t target_count = 0;
const char* output_path = nullptr;

__attribute__((no_instrument_function)) void Initialize() {
  const char* raw_targets = std::getenv("PERFLOOP_COUNTER_ADDRS");
  output_path = std::getenv("PERFLOOP_COUNTER_FILE");
  if (raw_targets == nullptr || output_path == nullptr)
    return;

  while (*raw_targets != '\0' && target_count < kMaxTargets) {
    char* end = nullptr;
    unsigned long long address = std::strtoull(raw_targets, &end, 16);
    if (end == raw_targets)
      return;

    targets[target_count++] = static_cast<uintptr_t>(address);
    raw_targets = (*end == ',') ? end + 1 : end;
  }
}

__attribute__((no_instrument_function)) void WriteResult() {
  if (output_path == nullptr)
    return;

  FILE* out = std::fopen(output_path, "w");
  if (out == nullptr)
    return;

  std::fprintf(out, "%llu\n", static_cast<unsigned long long>(callback_entries.load()));
  std::fclose(out);
}

__attribute__((constructor, no_instrument_function)) void CounterConstructor() {
  Initialize();
}

__attribute__((destructor, no_instrument_function)) void CounterDestructor() {
  WriteResult();
}

}  // namespace

extern "C" __attribute__((no_instrument_function)) void __cyg_profile_func_enter(void* this_fn,
                                                                                 void*) {
  const uintptr_t address = reinterpret_cast<uintptr_t>(this_fn);
  for (size_t i = 0; i < target_count; ++i) {
    if (address == targets[i]) {
      callback_entries.fetch_add(1, std::memory_order_relaxed);
      return;
    }
  }
}

extern "C" __attribute__((no_instrument_function)) void __cyg_profile_func_exit(void*, void*) {
}
