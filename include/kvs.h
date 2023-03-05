/**
 * @file kvs.h
 * @brief This is the interface used by outside.
 */

#pragma once

#include <tuple>

#include "base_node.h"
#include "interface_destroy.h"
#include "interface_get.h"
#include "interface_helper.h"
#include "interface_put.h"
#include "interface_remove.h"
#include "interface_scan.h"
#include "storage.h"
#include "storage_impl.h"

namespace yakushima {

/**
 * @brief Initialize kThreadInfoTable which is a table that holds thread execution
 * information about garbage collection and invoke epoch thread.
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

/**
 * @param [in] storage_name
 * @return The memory usage data of the given storage.
 */
[[maybe_unused]] static memory_usage_stack
mem_usage(std::string_view storage_name); // NOLINT

/**
 * @brief Create storage
 * @param [in] storage_name
 * @attention Do not treat DDL operations in parallel with DML operations.
 * create_storage / delete_storage can be processed in parallel.
 * At least one of these and find_storage / list_storage cannot work in parallel.
 * @return Same to put function.
 */
[[maybe_unused]] static status create_storage(std::string_view storage_name) {
    return storage::create_storage(storage_name);
}

/**
 * @brief Delete existing storage and values under the storage
 * @param [in] storage_name
 * @attention Do not treat DDL operations in parallel with DML operations.
 * create_storage / delete_storage can be processed in parallel.
 * At least one of these and find_storage / list_storage cannot work in parallel.
 * @return status::OK if successful.
 * @return status::WARN_CONCURRENT_OPERATIONS if it can find the storage,
 * but This function failed because it was preceded by concurrent delete_storage.
 * After this, if create_storage is executed with the same storage name, the storage
 * exists, and if it is not executed, the storage should not exist.
 * @return status::WARN_NOT_EXIST if the storage was not found.
 */
[[maybe_unused]] static status delete_storage(std::string_view storage_name) {
    return storage::delete_storage(storage_name);
}

/**
 * @brief Find existing storage
 * @param [in] storage_name
 * @param [out] found_storage output parameter to pass tree_instance information. If this
 * is nullptr (by default argument), this function simply note the existence of target.
 * @attention Do not treat DDL operations in parallel with DML operations.
 * create_storage / delete_storage can be processed in parallel.
 * At least one of these and find_storage / list_storage cannot work in parallel.
 * @return status::OK if existence.
 * @return status::WARN_NOT_EXIST if not existence.
 */
[[maybe_unused]] static status
find_storage(std::string_view storage_name,
             tree_instance** found_storage = nullptr) {
    return storage::find_storage(storage_name, found_storage);
}

/**
 * @brief List existing storage
 * @param [out] out output parameter to pass list of existing storage.
 * @attention Do not treat DDL operations in parallel with DML opeartions.
 * create_storage / delete_storage can be processed in parallel.
 * At least one of these and find_storage / list_storage cannot work in parallel.
 * @return status::OK if it found.
 * @return status::WARN_NOT_EXIST if it found no storage.
 */
[[maybe_unused]] static status
list_storages(std::vector<std::pair<std::string, tree_instance*>>& out) {
    return storage::list_storages(out);
}

/**
 * @details It declares that the session starts. In a session defined as between enter and
 * leave, it is guaranteed that the heap memory object object read by get function will
 * not be released in session. An occupied GC container is assigned.
 * @param[out] token If the return value of the function is status::OK, then the token is
 * the acquired session.
 * @return status::OK success.
 * @return status::WARN_MAX_SESSIONS The maximum number of sessions is already up and
 * running.
 */
[[maybe_unused]] static status enter(Token& token); // NOLINT

/**
 * @details It declares that the session ends. Values read during the session may be
 * invalidated from now on. It will clean up the contents of GC containers that have been
 * occupied by this session as much as possible.
 * @param[in] token
 * @return status::OK success
 * @return status::WARN_INVALID_TOKEN @a token of argument is invalid.
 */
[[maybe_unused]] static status leave(Token token); // NOLINT

/**
 * @brief Get value which is corresponding to given @a key_view.
 * @tparam ValueType The returned pointer is cast to the given type information before it
 * is returned.
 * @param[in] storage_name The key_view of storage name.
 * @param[in] key_view The key_view of key-value.
 * @param[out] out The result about pointer to value and value size.
 * @return std::status::OK success
 * @return status::WARN_NOT_EXIST The target storage of this operation exists, 
 * but the target entry of the storage does not exist.
 * @return status::WARN_STORAGE_NOT_EXIST The target storage of this operation 
 * does not exist.
 */
template<class ValueType>
[[maybe_unused]] static status get(std::string_view storage_name, // NOLINT
                                   std::string_view key_view,
                                   std::pair<ValueType*, std::size_t>& out);

/**
 * @biref Put the value with given @a key_view.
 * @pre @a token of arguments is valid.
 * @tparam ValueType If a single object is inserted, the value size and value alignment
 * information can be omitted from this type information. In this case, sizeof and alignof
 * are executed on the type information. In the cases where this is likely to cause
 * problems and when inserting_deleting an array object, the value size and value
 * alignment information should be specified explicitly. This is because sizeof for a type
 * represents a single object size.
 * @param[in] token todo write
 * @param[in] storage_name todo write
 * @param[in] key_view The key_view of key-value.
 * @param[in] value_ptr The pointer to given value.
 * @param[out] created_value_ptr The pointer to created value in yakushima. Default is @a
 * nullptr.
 * @param[in] arg_value_length The length of value object. Default is @a
 * sizeof(ValueType).
 * @param[in] value_align The alignment information of value object. Default is @a
 * static_cast<value_align_type>(alignof(ValueType)).
 * @param[in] unique_restriction If this is true, you can't put same key. If you
 * update key, you should execute remove and put.
 * @param[out] inserted_node_version_ptr The pointer to version of the inserted 
 * node. It may be used to find out difference of the version between some 
 * operations. Default is @a nullptr. If split occurs due to this insert, this
 *  point to old border node.
 * @return status::OK success.
 * @return status::WARN_UNIQUE_RESTRICTION The key-value whose key is same to given key
 * already exists.
 * @return status::WARN_STORAGE_NOT_EXIST The target storage of this operation 
 * does not exist.
 */
template<class ValueType>
[[maybe_unused]] static status
put(Token token, std::string_view storage_name, // NOLINT
    std::string_view key_view, ValueType* value_ptr,
    std::size_t arg_value_length,
    ValueType** created_value_ptr, // NOLINT
    value_align_type value_align, bool unique_restriction,
    node_version64** inserted_node_version_ptr);

/**
 * @pre @a token of arguments is valid.
 * @param[in] token
 * @param[in] key_view The key_view of key-value.
 * @return status::OK success
 * @return status::OK_ROOT_IS_NULL No existing tree.
 * @return status::OK_NOT_FOUND The target storage exists, but the target 
 * entry does not exist.
 * @return status::WARN_STORAGE_NOT_EXIST The target storage of this operation 
 * does not exist.
 */
[[maybe_unused]] static status remove(Token token, // NOLINT
                                      std::string_view storage_name,
                                      std::string_view key_view);

/**
 * TODO : add new 3 modes : try-mode : 1 trial : wait-mode : try until success : mid-mode
 * : middle between try and wait.
 */
/**
 * @brief scan range between @a l_key and @a r_key.
 * @tparam ValueType The returned pointer is cast to the given type information 
 * before it is returned.
 * @param[in] l_key An argument that specifies the left endpoint.
 * @param[in] l_end If this argument is scan_endpoint::EXCLUSIVE, the interval 
 * does not include the endpoint. If this argument is scan_endpoint::INCLUSIVE, 
 * the interval contains the endpoint. If this is scan_endpoint::INF, there is 
 * no limit on the interval in left direction. And ignore @a l_key.
 * @param[in] r_key An argument that specifies the right endpoint.
 * @note If r_key <l_key is specified in dictionary order, nothing will be hit.
 * @param[in] r_end If this argument is scan_endpoint :: EXCLUSIVE, the 
 * interval does not include the endpoint. If this argument is 
 * scan_endpoint::INCLUSIVE, the interval contains the endpoint. If this is 
 * scan_endpoint::INF, there is no limit on the interval in right direction. 
 * And ignore @a r_key.
 * @param[out] tuple_list A set with a key, a pointer to value, and size of 
 * value as a result of this function.
 * @param[out] node_version_vec Default is nullptr. The set of node_version for
 * transaction processing (protection of phantom problems). If you don't use 
 * yakushima for transaction processing, you don't have to use this argument. 
 * If you want high performance and don't have to use this argument, you should 
 * specify nullptr. If this argument is nullptr, it will not collect 
 * node_version.
 * @param [out] max_size Default is 0. If this argument is 0, it will not use 
 * this argument. This argument limits the number of results. If you later use 
 * node_version_vec to guarantee atomicity, you can split the atomic scan. 
 * Suppose you want to scan with A: C, assuming the order A <B <C. You can 
 * perform an atomic scan by scanning the range A: B, B: C and using 
 * node_version_vec to make sure the values ​​are not overwritten. This advantage 
 * is effective when the right end point is unknown but you want to scan to a 
 * specific value.
 * @return Status::ERR_BAD_USAGE The arguments is invalid. In the case1: you use
 * same l_key and r_key and one of the endpoint is exclusive. case2: one of the
 * endpoint use null key but the string size is not zero like 
 * std::string_view{nullptr, non-zero-value}.
 * @return status::OK success.
 * @return Status::OK_ROOT_IS_NULL success and root is null.
 * @return status::WARN_STORAGE_NOT_EXIST The target storage of this operation 
 * does not exist.
 */
template<class ValueType>
[[maybe_unused]] static status
scan(std::string_view storage_name, std::string_view l_key, // NOLINT
     scan_endpoint l_end, std::string_view r_key, scan_endpoint r_end,
     std::vector<std::tuple<std::string, ValueType*, std::size_t>>& tuple_list,
     std::vector<std::pair<node_version64_body, node_version64*>>*
             node_version_vec,
     std::size_t max_size);

} // namespace yakushima
