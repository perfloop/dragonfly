// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/hnsw_index.h"

#include <benchmark/benchmark.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <span>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "core/search/stateless_allocator.h"

namespace dfly::search {

#ifdef HNSW_BENCH_COUNT_DISTANCES
namespace {

thread_local uint64_t g_distance_computations = 0;

}  // namespace

extern "C" float RealVectorDistance(const void*, const void*, size_t, VectorSimilarity,
                                     VectorDataType)
    asm("__real__ZN4dfly6search14VectorDistanceEPKvS2_mNS0_16VectorSimilarityENS0_14VectorDataTypeE");
extern "C" float WrappedVectorDistance(const void*, const void*, size_t, VectorSimilarity,
                                        VectorDataType)
    asm("__wrap__ZN4dfly6search14VectorDistanceEPKvS2_mNS0_16VectorSimilarityENS0_14VectorDataTypeE");

extern "C" float WrappedVectorDistance(const void* lhs, const void* rhs, size_t dim,
                                        VectorSimilarity sim, VectorDataType data_type) {
  ++g_distance_computations;
  return RealVectorDistance(lhs, rhs, dim, sim, data_type);
}
#endif

namespace {

constexpr std::string_view kFieldName = "vector";
constexpr size_t kBenchmarkDimension = 64;
constexpr size_t kBenchmarkLiveDocs = 512;
constexpr size_t kBenchmarkChurnOps = 1024;
constexpr uint32_t kBenchmarkEf = 64;

template <size_t Dimension> using Vector = std::array<float, Dimension>;

template <size_t Dimension> class BorrowedVectorDoc : public DocumentAccessor {
 public:
  explicit BorrowedVectorDoc(const Vector<Dimension>& vector) : vector_{vector} {
  }

  std::optional<StringList> GetStrings(std::string_view) const override {
    return std::nullopt;
  }

  std::optional<VectorInfo> GetVector(std::string_view, size_t dim,
                                      VectorDataType data_type) const override {
    if (dim != Dimension || data_type != VectorDataType::FLOAT32)
      return std::nullopt;
    return VectorInfo{BorrowedFtVector{reinterpret_cast<const char*>(vector_.data())}};
  }

  std::optional<NumsList> GetNumbers(std::string_view) const override {
    return std::nullopt;
  }

  std::optional<StringList> GetTags(std::string_view) const override {
    return std::nullopt;
  }

 private:
  const Vector<Dimension>& vector_;
};

class SearchMemoryScope {
 public:
  SearchMemoryScope() {
    InitTLSearchMR(PMR_NS::get_default_resource());
  }

  ~SearchMemoryScope() {
    InitTLSearchMR(nullptr);
  }
};

template <size_t Dimension> std::vector<Vector<Dimension>> MakeVectors(size_t count) {
  std::mt19937 rng{0x4f6b4f1u};
  std::uniform_real_distribution<float> distribution{-1.0f, 1.0f};
  std::vector<Vector<Dimension>> vectors(count);

  for (auto& vector : vectors) {
    for (float& value : vector)
      value = distribution(rng);
  }
  return vectors;
}

SchemaField::VectorParams MakeParams(size_t dimension, size_t capacity) {
  SchemaField::VectorParams params;
  params.use_hnsw = true;
  params.dim = dimension;
  params.sim = VectorSimilarity::L2;
  params.capacity = capacity;
  params.hnsw_m = 16;
  params.hnsw_ef_construction = 100;
  params.hnsw_ef_runtime = kBenchmarkEf;
  return params;
}

template <size_t Dimension>
std::unique_ptr<HnswVectorIndex> BuildIndex(const std::vector<Vector<Dimension>>& vectors,
                                            size_t live_docs, bool copy_vector) {
  auto index = std::make_unique<HnswVectorIndex>(MakeParams(Dimension, live_docs), copy_vector);
  for (size_t i = 0; i < live_docs; ++i) {
    BorrowedVectorDoc<Dimension> doc{vectors[i]};
    if (!index->Add(static_cast<GlobalDocId>(i), doc, kFieldName))
      return nullptr;
  }
  return index;
}

template <size_t Dimension>
std::unique_ptr<HnswVectorIndex> BuildChurnedIndex(const std::vector<Vector<Dimension>>& vectors,
                                                   size_t live_docs, size_t churn_ops,
                                                   bool copy_vector) {
  auto index = BuildIndex(vectors, live_docs, copy_vector);
  if (!index)
    return nullptr;

  std::vector<GlobalDocId> live_ids(live_docs);
  std::iota(live_ids.begin(), live_ids.end(), GlobalDocId{0});

  for (size_t op = 0; op < churn_ops; ++op) {
    size_t slot = op % live_docs;
    index->Remove(live_ids[slot]);

    BorrowedVectorDoc<Dimension> doc{vectors[live_docs + op]};
    GlobalDocId id = static_cast<GlobalDocId>(live_docs + op);
    if (!index->Add(id, doc, kFieldName))
      return nullptr;
    live_ids[slot] = id;
  }
  return index;
}

bool ContainsId(const std::vector<std::pair<float, GlobalDocId>>& results, GlobalDocId id) {
  return std::any_of(results.begin(), results.end(),
                     [id](const auto& result) { return result.second == id; });
}

uint64_t P99(std::vector<uint64_t> samples) {
  DCHECK(!samples.empty());
  size_t percentile_index = (samples.size() * 99 + 99) / 100 - 1;
  std::nth_element(samples.begin(), samples.begin() + percentile_index, samples.end());
  return samples[percentile_index];
}

TEST(HnswDeletedSlotReuse, PreservesSameLabelReactivationAndLiveResults) {
  SearchMemoryScope memory_scope;
  constexpr size_t kDimension = 4;
  auto vectors = MakeVectors<kDimension>(7);
  auto index = BuildIndex(vectors, /*live_docs=*/4, /*copy_vector=*/true);
  ASSERT_NE(index, nullptr);

  index->Remove(0);
  index->Remove(1);

  // A same-label reactivation must not consume the other deleted label.
  BorrowedVectorDoc<kDimension> reactivated_doc{vectors[4]};
  ASSERT_TRUE(index->Add(0, reactivated_doc, kFieldName));

  BorrowedVectorDoc<kDimension> replacement_doc{vectors[5]};
  ASSERT_TRUE(index->Add(10, replacement_doc, kFieldName));

  // The reactivated label must remain removable and insertable after an unrelated replacement.
  index->Remove(0);
  BorrowedVectorDoc<kDimension> final_doc{vectors[6]};
  ASSERT_TRUE(index->Add(0, final_doc, kFieldName));

  auto results = index->Knn(vectors[6].data(), /*k=*/4, /*ef=*/32);
  EXPECT_TRUE(ContainsId(results, 0));
  EXPECT_TRUE(ContainsId(results, 10));
  EXPECT_FALSE(ContainsId(results, 1));
}

TEST(HnswDeletedSlotReuse, RestoredBorrowedVectorsRemainSearchableAfterChurn) {
  SearchMemoryScope memory_scope;
  constexpr size_t kDimension = 4;
  constexpr size_t kLiveDocs = 8;
  auto vectors = MakeVectors<kDimension>(kLiveDocs + 1);

  auto original = BuildIndex(vectors, kLiveDocs, /*copy_vector=*/false);
  ASSERT_NE(original, nullptr);

  std::vector<HnswNodeData> nodes;
  {
    auto lock = original->GetReadLock();
    nodes = original->GetNodesRange(0, original->GetNodeCount());
  }
  HnswIndexMetadata metadata = original->GetMetadata();

  HnswVectorIndex restored(MakeParams(kDimension, kLiveDocs), /*copy_vector=*/false);
  ASSERT_TRUE(restored.RestoreFromNodes(nodes, metadata));
  for (size_t i = 0; i < kLiveDocs; ++i) {
    BorrowedVectorDoc<kDimension> doc{vectors[i]};
    ASSERT_TRUE(restored.UpdateVectorData(i, doc, kFieldName));
  }

  restored.Remove(0);
  BorrowedVectorDoc<kDimension> replacement_doc{vectors[kLiveDocs]};
  ASSERT_TRUE(restored.Add(100, replacement_doc, kFieldName));

  auto results = restored.Knn(vectors[kLiveDocs].data(), /*k=*/kLiveDocs, /*ef=*/32);
  EXPECT_TRUE(ContainsId(results, 100));
  EXPECT_FALSE(ContainsId(results, 0));
}

TEST(HnswDeletedSlotReuse, SerializesConcurrentDistinctLabelChurn) {
  SearchMemoryScope memory_scope;
  constexpr size_t kDimension = 4;
  constexpr size_t kLiveDocs = 8;
  constexpr size_t kWorkers = 2;
  constexpr size_t kChurnOpsPerWorker = 64;
  auto vectors = MakeVectors<kDimension>(kLiveDocs + kWorkers * kChurnOpsPerWorker);
  auto index = BuildIndex(vectors, kLiveDocs, /*copy_vector=*/false);
  ASSERT_NE(index, nullptr);

  std::atomic<bool> start = false;
  std::vector<std::thread> workers;
  workers.reserve(kWorkers);
  for (size_t worker = 0; worker < kWorkers; ++worker) {
    workers.emplace_back([&, worker] {
      while (!start.load(std::memory_order_acquire))
        std::this_thread::yield();

      GlobalDocId current_id = static_cast<GlobalDocId>(worker);
      for (size_t op = 0; op < kChurnOpsPerWorker; ++op) {
        index->Remove(current_id);
        size_t vector_index = kLiveDocs + worker * kChurnOpsPerWorker + op;
        current_id = static_cast<GlobalDocId>(vector_index);
        BorrowedVectorDoc<kDimension> doc{vectors[vector_index]};
        EXPECT_TRUE(index->Add(current_id, doc, kFieldName));
      }
    });
  }

  start.store(true, std::memory_order_release);
  for (auto& worker : workers)
    worker.join();

  for (size_t worker = 0; worker < kWorkers; ++worker) {
    size_t vector_index = kLiveDocs + (worker + 1) * kChurnOpsPerWorker - 1;
    auto results = index->Knn(vectors[vector_index].data(), /*k=*/kLiveDocs, /*ef=*/64);
    EXPECT_TRUE(ContainsId(results, static_cast<GlobalDocId>(vector_index)));
  }
}

template <bool CopyVector> void BM_HnswDeletedSlotChurnAddImpl(benchmark::State& state) {
  SearchMemoryScope memory_scope;
  const auto vectors =
      MakeVectors<kBenchmarkDimension>(kBenchmarkLiveDocs + kBenchmarkChurnOps);
  std::vector<uint64_t> add_latencies_ns;
  add_latencies_ns.reserve(16 * kBenchmarkChurnOps);
  uint64_t resize_retry_count = 0;
  size_t graph_nodes = 0;
  size_t deleted_nodes = 0;
  size_t graph_bytes = 0;

  for (auto _ : state) {
    state.PauseTiming();
    auto index = BuildIndex(vectors, kBenchmarkLiveDocs, CopyVector);
    if (!index) {
      state.SkipWithError("failed to build HNSW index");
      return;
    }

    std::vector<GlobalDocId> live_ids(kBenchmarkLiveDocs);
    std::iota(live_ids.begin(), live_ids.end(), GlobalDocId{0});
    size_t next_capacity = kBenchmarkLiveDocs;
    state.ResumeTiming();

    for (size_t op = 0; op < kBenchmarkChurnOps; ++op) {
      size_t slot = op % kBenchmarkLiveDocs;
      state.PauseTiming();
      index->Remove(live_ids[slot]);
      state.ResumeTiming();

      BorrowedVectorDoc<kBenchmarkDimension> doc{vectors[kBenchmarkLiveDocs + op]};
      GlobalDocId id = static_cast<GlobalDocId>(kBenchmarkLiveDocs + op);
      const auto start = std::chrono::steady_clock::now();
      bool added = index->Add(id, doc, kFieldName);
      const auto end = std::chrono::steady_clock::now();

      state.PauseTiming();
      add_latencies_ns.push_back(
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
      if (!added) {
        state.SkipWithError("failed to add HNSW replacement vector");
        return;
      }
      live_ids[slot] = id;
      const size_t node_count = index->GetNodeCount();
      // In this workload, exceeding the prior capacity means DoAdd caught its
      // capacity exception, doubled the graph, and retried this insertion once.
      while (node_count > next_capacity) {
        ++resize_retry_count;
        next_capacity *= 2;
      }
      state.ResumeTiming();
    }

    state.PauseTiming();
    auto results = index->Knn(vectors.back().data(), /*k=*/10, kBenchmarkEf);
    if (results.empty()) {
      state.SkipWithError("churned HNSW index returned no query results");
      return;
    }
    graph_nodes = index->GetNodeCount();
    // Each churn operation replaces one live label with a new label, so every
    // node above the fixed live population is a deleted tombstone.
    deleted_nodes = graph_nodes - kBenchmarkLiveDocs;
    graph_bytes = index->GetMemoryUsage();
    index.reset();
    state.ResumeTiming();
  }

  state.SetItemsProcessed(state.iterations() * kBenchmarkChurnOps);
  state.counters["add_p99_ns"] = benchmark::Counter(static_cast<double>(P99(add_latencies_ns)));
  state.counters["graph_nodes"] = benchmark::Counter(static_cast<double>(graph_nodes));
  state.counters["deleted_nodes"] = benchmark::Counter(static_cast<double>(deleted_nodes));
  state.counters["graph_bytes"] = benchmark::Counter(static_cast<double>(graph_bytes));
  state.counters["resize_retry_count"] =
      benchmark::Counter(static_cast<double>(resize_retry_count) / state.iterations());
}

static void BM_HnswDeletedSlotChurnAddCopied(benchmark::State& state) {
  BM_HnswDeletedSlotChurnAddImpl</*CopyVector=*/true>(state);
}

static void BM_HnswDeletedSlotChurnAddBorrowed(benchmark::State& state) {
  BM_HnswDeletedSlotChurnAddImpl</*CopyVector=*/false>(state);
}

BENCHMARK(BM_HnswDeletedSlotChurnAddCopied)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_HnswDeletedSlotChurnAddBorrowed)->Unit(benchmark::kMicrosecond);

template <bool CopyVector> void BM_HnswDeletedSlotChurnKnnImpl(benchmark::State& state) {
  SearchMemoryScope memory_scope;
  const auto vectors =
      MakeVectors<kBenchmarkDimension>(kBenchmarkLiveDocs + kBenchmarkChurnOps);
  auto index = BuildChurnedIndex(vectors, kBenchmarkLiveDocs, kBenchmarkChurnOps, CopyVector);
  if (!index) {
    state.SkipWithError("failed to build churned HNSW index");
    return;
  }

  if (index->Knn(vectors.back().data(), /*k=*/10, kBenchmarkEf).empty()) {
    state.SkipWithError("churned HNSW index returned no query results");
    return;
  }

#ifdef HNSW_BENCH_COUNT_DISTANCES
  g_distance_computations = 0;
#endif
  for (auto _ : state) {
    auto results = index->Knn(vectors.back().data(), /*k=*/10, kBenchmarkEf);
    benchmark::DoNotOptimize(results);
    benchmark::ClobberMemory();
  }
#ifdef HNSW_BENCH_COUNT_DISTANCES

  state.counters["distance_computations_per_query"] =
      benchmark::Counter(static_cast<double>(g_distance_computations) / state.iterations());
#endif
}

static void BM_HnswDeletedSlotChurnKnnCopied(benchmark::State& state) {
  BM_HnswDeletedSlotChurnKnnImpl</*CopyVector=*/true>(state);
}

static void BM_HnswDeletedSlotChurnKnnBorrowed(benchmark::State& state) {
  BM_HnswDeletedSlotChurnKnnImpl</*CopyVector=*/false>(state);
}

BENCHMARK(BM_HnswDeletedSlotChurnKnnCopied)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_HnswDeletedSlotChurnKnnBorrowed)->Unit(benchmark::kMicrosecond);

}  // namespace
}  // namespace dfly::search
