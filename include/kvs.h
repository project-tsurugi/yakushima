/**
 * @file kvs.h
 * @brief This is the interface used by outside.
 */

#pragma once

#include "base_node.h"
#include "interface_get.h"
#include "interface_helper.h"
#include "interface_put.h"
#include "interface_remove.h"
#include "interface_scan.h"
#include "storage.h"
#include "storage_impl.h"


namespace yakushima {

/**
 * @brief Initialize kThreadInfoTable which is a table that holds thread execution information about garbage
 * collection and invoke epoch thread.
 */
[[maybe_unused]] static void init(); // NOLINT

/**
 * @brief Delete all tree from the root, release all heap objects, and join epoch thread.
 */
[[maybe_unused]] static void fin(); // NOLINT

/**
 * @brief release all heap objects and clean up.
 * @pre This function is called by single thread.
 * @return status::OK_DESTROY_ALL destroyed all tree.
 * @return status::OK_ROOT_IS_NULL tree was nothing.
 */
[[maybe_unused]] static status destroy(); // NOLINT

[[maybe_unused]] static status create_storage(std::string_view storage_name) {
    return storage::create_storage(storage_name);
}

[[maybe_unused]] static status delete_storage(std::string_view storage_name) {
    return storage::delete_storage(storage_name);
}

[[maybe_unused]] static status find_storage(std::string_view storage_name, std::atomic<base_node*>** found_storage) {
    return storage::find_storage(storage_name, found_storage);
}

/**
 * @details It declares that the session starts. In a session defined as between enter and leave, it is guaranteed
 * that the heap memory object object read by get function will not be released in session. An occupied GC container
 * is assigned.
 * @param[out] token If the return value of the function is status::OK, then the token is the acquired session.
 * @return status::OK success.
 * @return status::WARN_MAX_SESSIONS The maximum number of sessions is already up and running.
 */
[[maybe_unused]] static status enter(Token &token); // NOLINT

/**
 * @details It declares that the session ends. Values read during the session may be invalidated from now on.
 * It will clean up the contents of GC containers that have been occupied by this session as much as possible.
 * @param[in] token
 * @return status::OK success
 * @return status::WARN_INVALID_TOKEN @a token of argument is invalid.
 */
[[maybe_unused]] static status leave(Token token); // NOLINT

/**
 * @brief Get value which is corresponding to given @a key_view.
 * @tparam ValueType The returned pointer is cast to the given type information before it is returned.
 * @param[in] key_view The key_view of key-value.
 * @return std::pair<ValueType *, std::size_t> The pair of pointer to value and the value size.
 */
template<class ValueType>
[[maybe_unused]] static std::pair<ValueType*, std::size_t> get(std::string_view storage_name, std::string_view key_view); // NOLINT

/**
 * @biref Put the value with given @a key_view.
 * @pre @a token of arguments is valid.
 * @tparam ValueType If a single object is inserted, the value size and value alignment information can be
 * omitted from this type information. In this case, sizeof and alignof are executed on the type information.
 * In the cases where this is likely to cause problems and when inserting_deleting an array object,
 * the value size and value alignment information should be specified explicitly.
 * This is because sizeof for a type represents a single object size.
 * @param[in] key_view The key_view of key-value.
 * @param[in] value The pointer to given value.
 * @param[out] created_value_ptr The pointer to created value in yakushima.
 * @param[in] arg_value_length The length of value object.
 * @param[in] value_align The alignment information of value object.
 * @param[out] inserted_node_version_ptr The pointer to version of the inserted node. It may be used to find out
 * difference of the version between some operations.
 * @return status::OK success.
 * @return status::WARN_UNIQUE_RESTRICTION The key-value whose key is same to given key already exists.
 */
template<class ValueType>
[[maybe_unused]] static status
put(std::string_view storage_name, std::string_view key_view, ValueType* value, std::size_t arg_value_length, ValueType** created_value_ptr, // NOLINT
    value_align_type value_align, node_version64** inserted_node_version_ptr);

/**
 * @pre @a token of arguments is valid.
 * @param[in] token
 * @param[in] key_view The key_view of key-value.
 * @return status::OK_ROOT_IS_NULL No existing tree.
 */
[[maybe_unused]] static status remove(Token token, std::string_view storage_name, std::string_view key_view); // NOLINT

/**
 * TODO : add new 3 modes : try-mode : 1 trial : wait-mode : try until success : mid-mode : middle between try and wait.
 */
/**
 * @brief scan range between @a l_key and @a r_key.
 * @tparam ValueType The returned pointer is cast to the given type information before it is returned.
 * @param[in] l_key An argument that specifies the left endpoint.
 * @param[in] l_end If this argument is scan_endpoint :: EXCLUSIVE, the interval does not include the endpoint.
 * If this argument is scan_endpoint :: INCLUSIVE, the interval contains the endpoint.
 * If this is scan_endpoint :: INF, there is no limit on the interval in left direction. And ignore @a l_key.
 * @param[in] r_key An argument that specifies the right endpoint.
 * @note If r_key <l_key is specified in dictionary order, nothing will be hit.
 * @param[in] r_end If this argument is scan_endpoint :: EXCLUSIVE, the interval does not include the endpoint.
 * If this argument is scan_endpoint :: INCLUSIVE, the interval contains the endpoint.
 * If this is scan_endpoint :: INF, there is no limit on the interval in right direction. And ignore @a r_key.
 * @param[out] tuple_list A set with a pointer to value and size as a result of this function.
 * @param[out] node_version_vec Default is nullptr. The set of node_version for transaction processing (protection of phantom problems).
 * If you don't use yakushima for transaction processing, you don't have to use this argument. 
 * If you want high performance and don't have to use this argument, you should specify nullptr.
 * If this argument is nullptr, it will not collect node_version.
 * @param [out] max_size Default is 0. If this argument is 0, it will not use this argument.
 * This argument limits the number of results. 
 * If you later use node_version_vec to guarantee atomicity, you can split the atomic scan. 
 * Suppose you want to scan with A: C, assuming the order A <B <C. 
 * You can perform an atomic scan by scanning the range A: B, B: C and using node_version_vec to make sure the values ​​are not overwritten. 
 * This advantage is effective when the right end point is unknown but you want to scan to a specific value.
 * @return status::OK success.
 */
template<class ValueType>
[[maybe_unused]] static status
scan(std::string_view storage_name, std::string_view l_key, scan_endpoint l_end, std::string_view r_key, scan_endpoint r_end, // NOLINT
     std::vector<std::pair<ValueType*, std::size_t>> &tuple_list,
     std::vector<std::pair<node_version64_body, node_version64*>>* node_version_vec, std::size_t max_size);

} // namespace yakushima
