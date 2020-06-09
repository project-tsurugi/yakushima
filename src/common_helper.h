/**
 * @file common_helper.h
 */

#pragma once

#include "interior_node.h"
#include "border_node.h"
#include "version.h"

namespace yakushima {

/**
 *
 * @details It finds border node by using arguments @a root, @a key_slice.
 * If the @a root is not the root of some layer, this function finds root nodes of the layer, then finds border node by
 * using retry label.
 * @param[in] root
 * @param[in] key_slice
 * @param[in] key_slice_length
 * @param[out] special_status
 * @return std::tuple<base_node*, node_version64_body>
 * node_version64_body is stable version of base_node*.
 */
static std::tuple<border_node *, node_version64_body>
find_border(base_node *root, key_slice_type key_slice, key_length_type key_slice_length, status &special_status) {
retry:
  base_node *n = root;
  node_version64_body v = n->get_stable_version();
  if (v.get_deleted() || !v.get_root()) {
    special_status = status::WARN_RETRY_FROM_ROOT_OF_ALL;
    return std::make_tuple(nullptr, node_version64_body());
  }
descend:
  /**
   * The caller checks whether it has been deleted.
   */
  if (n->get_version_border())
    return std::make_tuple(static_cast<border_node *>(n), v);
  /**
   * @a n points to a interior_node object.
   */
  base_node *n_child = static_cast<interior_node *>(n)->get_child_of(key_slice, key_slice_length);
  /**
   * As soon as you it finished operating the contents of node, read version (v_check).
   */
  node_version64_body v_check = n->get_stable_version();
  if (v == v_check) {
    /**
     * This check is different with original paper's check.
     * Original paper approach merely check whether it is locked now.
     */
    n = n_child;
    /**
     * In the original paper, this line is done outside of if scope.
     * However,  It is never used after that scope.
     * Therefore, this can execute within if scope.
     */
    v = n_child->get_stable_version();
    goto descend;
  }
  /**
   * In Original paper, v'' = stableversion(n).
   * But it is redundant that checks global current value (n.version) and loads that here.
   */

  /**
   * If the value of vsplit is different, the read location may be inappropriate.
   * Split propagates upward. It have to start from root.
   */
  if (v.get_vsplit() != v_check.get_vsplit())
    goto retry;
  /**
   * If the vsplit values ​​match, it can continue.
   * Even if it is inserted, the slot value is invariant and the order is controlled by @a permutation.
   * However, the value corresponding to the key that could not be detected may have been inserted,
   * so re-start from this node.
   */
  v = v_check;
  goto descend;
}

} // namespace yakushima

