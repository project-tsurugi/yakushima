#pragma once

/**
 * @file
 * @brief atomic wrapper of builtin function.
 */

/**
 * @brief atomic relaxed load.
 */
template <typename T>
T loadRelaxed(T& ptr) {
  return __atomic_load_n(&ptr, __ATOMIC_RELAXED);
}

/**
 * @brief atomic acquire load.
 */
template<typename T>
T loadAcquireN(T &ref) {
  return __atomic_load_n(&ref, __ATOMIC_ACQUIRE);
}

#if 0
template<class type>
void loadAcquire(type *ptr, type *ret) {
  __atomic_load(ptr, ret, __ATOMIC_ACQUIRE);
}
#endif

/**
 * @brief atomic relaxed store.
 */
template<typename T, typename T2>
void storeRelaxed(T &ptr, T2 val) {
  __atomic_store_n(&ptr, (T) val, __ATOMIC_RELAXED);
}

/**
 * @brief atomic release store.
 */
template<typename T, typename T2>
void storeReleaseN(T &ptr, T2 val) {
  __atomic_store_n(&ptr, (T) val, __ATOMIC_RELEASE);
}

#if 0
template<class type>
void storeRelease(type *ptr, type *val) {
  __atomic_store(ptr, val, __ATOMIC_RELEASE);
}

/**
 * @brief atomic acq-rel cas.
 */
template<typename type>
bool weakCompareExchange(type *ptr, type *expected, type *desired) {
  /**
   * Built-in Function: bool __atomic_compare_exchange_n
   * (type *ptr, type *expected, type desired, bool weak, int success_memorder, int failure_memorder)
   */
  return __atomic_compare_exchange(ptr, expected, desired, true,
                                   __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}
#endif
