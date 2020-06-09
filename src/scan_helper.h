/**
 * @file scan_helper.h
 */

#pragma once

#include "base_node.h"
#include "border_node.h"
#include "interior_node.h"
#include "scheme.h"

namespace yakushima {

template<class ValueType>
status scan_check_retry(border_node *const bn, const node_version64_body &v_at_fetch_lv,
                        std::tuple<ValueType *, std::size_t> &tuple_list, const std::size_t &tuple_pushed_num) {
  node_version64_body check = bn->get_stable_version();
  if (check != v_at_fetch_lv) {
    if (tuple_pushed_num != 0) {
      tuple_list.erase(tuple_list.end() - tuple_pushed_num, tuple_list.end());
    }
    if (check.get_vsplit() != v_at_fetch_lv.get_vsplit() ||
        check.get_deleted()) {
      return status::OK_RETRY_FROM_ROOT;
    } else {
      return status::OK_RETRY_FETCH_LV;
    }
  }
  return status::OK;
}

template<class ValueType>
static status scan_all(base_node *root, std::tuple<ValueType *, std::size_t> &tuple_list,
                       std::size_t &tuple_pushed_num) {
  return status::OK;
}

template<class ValueType>
static status scan_border(border_node **target, std::string_view l_key, bool l_exclusive, std::string_view r_key,
                          bool r_exclusive, std::tuple<ValueType *, std::size_t> &tuple_list,
                          node_version64_body &v_at_fetch_lv) {
  std::size_t tuple_pushed_num{0};
  border_node *bn = *target;
  border_node *next = bn->get_next();
  permutation perm(bn->get_permutation().get_body());
  for (std::size_t i = 0; i < perm.get_cnk(); ++i) {
    std::size_t index = perm.get_index_of_rank(i);
    link_or_value *lv = bn->get_lv_at(index);
    void *vp = lv->get_v_or_vp_();
    std::size_t vsize = lv->get_value_length();
    base_node *next_layer = lv->get_next_layer();
    key_length_type kl = bn->get_key_length_at(index);
    status check_status = scan_check_retry(bn, v_at_fetch_lv, tuple_list, tuple_pushed_num);
    if (check_status == status::OK_RETRY_FROM_ROOT) {
      return status::OK_RETRY_FROM_ROOT;
    } else if (check_status == status::OK_RETRY_FETCH_LV) {
      return status::OK_RETRY_FETCH_LV;
    }
    if (kl > sizeof(key_slice_type)) {
      if (scan_all(next_layer, tuple_list, tuple_pushed_num) == status::OK_RETRY_FETCH_LV) {
        return status::OK_RETRY_FETCH_LV;
      }
    } else {
      tuple_list.emplace_back(std::make_tuple(reinterpret_cast<ValueType *>(vp), vsize));
      ++tuple_pushed_num;
    }
  }

  if (next == nullptr) {
    return status::OK_SCAN_END;
  } else {
    *target = next;
    return status::OK_SCAN_CONTINUE;
  }
}

} // namespace yakushima