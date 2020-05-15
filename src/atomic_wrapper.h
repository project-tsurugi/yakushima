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
template <typename T>
T loadAcquire(T& ref) {
  return __atomic_load_n(&ref, __ATOMIC_ACQUIRE);
}

/**
 * @brief atomic relaxed store.
 */
template <typename T, typename T2>
void storeRelaxed(T& ptr, T2 val) {
  __atomic_store_n(&ptr, (T)val, __ATOMIC_RELAXED);
}

/**
 * @brief atomic release store.
 */
template <typename T, typename T2>
void storeRelease(T& ptr, T2 val) {
  __atomic_store_n(&ptr, (T)val, __ATOMIC_RELEASE);
}

/**
 * @brief atomic acq-rel cas.
 */
template <typename T, typename T2>
bool compareExchange(T& m, T& before, T2 after) {
  return __atomic_compare_exchange_n(&m, &before, (T)after, false,
                                     __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}
