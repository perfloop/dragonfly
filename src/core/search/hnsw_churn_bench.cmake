# Adds the HNSW churn benchmark without changing the production CMake target list.
# This file is loaded through CMAKE_PROJECT_INCLUDE by the performance-proof build.
# CMake loads CMAKE_PROJECT_INCLUDE for third-party subprojects too; schedule once.
get_property(_hnsw_churn_bench_scheduled GLOBAL PROPERTY hnsw_churn_bench_scheduled)
if(_hnsw_churn_bench_scheduled)
  return()
endif()
set_property(GLOBAL PROPERTY hnsw_churn_bench_scheduled TRUE)
set_property(GLOBAL PROPERTY hnsw_churn_bench_source
             "${CMAKE_CURRENT_LIST_DIR}/hnsw_churn_bench.cc")

function(_add_hnsw_churn_bench target count_distances)
  get_property(source GLOBAL PROPERTY hnsw_churn_bench_source)
  add_executable(${target} "${source}")
  add_include(${target} ${GTEST_INCLUDE_DIR} ${BENCHMARK_INCLUDE_DIR})
  target_compile_definitions(${target} PRIVATE _TEST_BASE_FILE_="hnsw_churn_bench.cc")
  cxx_link(${target} gtest_main_ext redis_test_lib dfly_search_core)

  if(count_distances)
    target_compile_definitions(${target} PRIVATE HNSW_BENCH_COUNT_DISTANCES=1)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
      target_link_options(${target} PRIVATE
        "-Wl,--wrap=_ZN4dfly6search14VectorDistanceEPKvS2_mNS0_16VectorSimilarityENS0_14VectorDataTypeE")
    endif()
  endif()

  if(WITH_SIMSIMD)
    target_link_libraries(${target} TRDP::simsimd)
    target_compile_definitions(${target} PRIVATE
      WITH_SIMSIMD=1
      SIMSIMD_DYNAMIC_DISPATCH=1
      SIMSIMD_NATIVE_F16=$<IF:$<BOOL:${SIMSIMD_NATIVE_F16}>,1,0>
      SIMSIMD_NATIVE_BF16=$<IF:$<BOOL:${SIMSIMD_NATIVE_F16}>,1,0>)
  endif()
endfunction()

function(_add_hnsw_churn_benches)
  _add_hnsw_churn_bench(hnsw_churn_bench FALSE)
  # The counter binary is not used for latency. It interposes VectorDistance
  # only to report the exact number of distance calls for the normal workload.
  _add_hnsw_churn_bench(hnsw_churn_distance_bench TRUE)
endfunction()

cmake_language(DEFER DIRECTORY "${CMAKE_SOURCE_DIR}" CALL _add_hnsw_churn_benches)
