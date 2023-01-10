/**
 * @file common_helper.h
 */

#pragma once

#include "border_node.h"
#include "interior_node.h"
#include "log.h"
#include "version.h"

#include "glog/logging.h"

namespace yakushima {

/**
 *
 * @details It finds border node by using arguments @a root, @a key_slice.
 * If the @a root is not the root of some layer, this function finds root nodes of the
 * layer, then finds border node by using retry label.
 * @param[in] root
 * @param[in] key_slice
 * @param[in] key_slice_length
 * @param[out] special_status
 * @return std::tuple<base_node*, node_version64_body>
 * node_version64_body is stable version of base_node*.
 */
static std::tuple<border_node*, const node_version64_body>
find_border(base_node* const root, const key_slice_type key_slice,
            const key_length_type key_slice_length, status& special_status) {
    special_status = status::OK;
retry:
    base_node* n = root;
    node_version64_body v = n->get_stable_version();
    if (!v.get_root()) {
        special_status = status::WARN_RETRY_FROM_ROOT_OF_ALL;
        return std::make_tuple(nullptr, node_version64_body());
    }
    if (v.get_deleted()) {
        // root && deleted node.
#ifndef NDEBUG
        if (n == nullptr) {
            LOG(ERROR) << log_location_prefix << "find_border: root: " << root
                       << ", key_slice: " << key_slice
                       << ", key_slice_length: " << key_slice_length
                       << ", special_status: " << special_status;
        }
#endif
        return std::make_tuple(dynamic_cast<border_node*>(n), v);
    }
    /**
     * The caller checks whether it has been deleted.
     */
    while (!v.get_border()) {
        /**
         * @a n points to a interior_node object.
         */
        base_node* n_child = dynamic_cast<interior_node*>(n)->get_child_of(
                key_slice, key_slice_length, v);
        if (n_child == nullptr) {
            /**
             * If the value of vsplit is different, the read location may be
             * inappropriate.
             * Split propagates upward. It have to start from root.
             */
            goto retry; // NOLINT
        }
        n = n_child;
    }
#ifndef NDEBUG
    if (n == nullptr) {
        LOG(ERROR) << log_location_prefix << "find_border: root: " << root
                   << ", key_slice: " << key_slice
                   << ", key_slice_length: " << key_slice_length
                   << ", special_status: " << special_status;
    }
#endif
    return std::make_tuple(dynamic_cast<border_node*>(n), v);
}

} // namespace yakushima