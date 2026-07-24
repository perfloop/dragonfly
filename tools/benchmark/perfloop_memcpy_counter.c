/*
 * Measurement-only LD_PRELOAD helper for perfloop_large_set.py.
 *
 * It records bytes passed to dynamically dispatched memcpy calls in the
 * Dragonfly process.  The regular benchmark starts a fresh server for every
 * sample, so the destructor writes one fixed-size little-endian snapshot when
 * that server exits.  This is intentionally not linked into Dragonfly.
 */
#define _GNU_SOURCE

#include <dlfcn.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

typedef void* (*memcpy_function)(void*, const void*, size_t);

struct CounterSnapshot {
  uint64_t calls;
  uint64_t bytes;
};

static _Atomic(memcpy_function) real_memcpy;
static _Atomic(uint64_t) memcpy_calls;
static _Atomic(uint64_t) memcpy_bytes;
static int counter_fd = -1;
static __thread int resolving_memcpy;

static void* FallbackMemcpy(void* destination, const void* source, size_t length) {
  volatile unsigned char* out = destination;
  const volatile unsigned char* in = source;
  for (size_t index = 0; index < length; ++index)
    out[index] = in[index];
  return destination;
}

static memcpy_function ResolveMemcpy(void) {
  memcpy_function result = atomic_load_explicit(&real_memcpy, memory_order_acquire);
  if (result || resolving_memcpy)
    return result;

  resolving_memcpy = 1;
  result = (memcpy_function)dlsym(RTLD_NEXT, "memcpy");
  resolving_memcpy = 0;
  if (result)
    atomic_store_explicit(&real_memcpy, result, memory_order_release);
  return result;
}

void* memcpy(void* destination, const void* source, size_t length) {
  memcpy_function implementation = ResolveMemcpy();
  void* result = implementation ? implementation(destination, source, length)
                                : FallbackMemcpy(destination, source, length);
  if (counter_fd >= 0) {
    atomic_fetch_add_explicit(&memcpy_calls, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&memcpy_bytes, length, memory_order_relaxed);
  }
  return result;
}

__attribute__((constructor)) static void InitializeCounter(void) {
  const char* counter_path = getenv("PERFLOOP_MEMCPY_COUNTER_PATH");
  if (counter_path)
    counter_fd = open(counter_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
}

__attribute__((destructor)) static void WriteCounter(void) {
  if (counter_fd < 0)
    return;

  struct CounterSnapshot snapshot = {
      .calls = atomic_load_explicit(&memcpy_calls, memory_order_relaxed),
      .bytes = atomic_load_explicit(&memcpy_bytes, memory_order_relaxed),
  };
  (void)write(counter_fd, &snapshot, sizeof(snapshot));
  (void)close(counter_fd);
}
