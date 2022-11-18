/**
 * @file tree_instance.h
 */

#pragma once

#include "atomic_wrapper.h"
#include "base_node.h"

namespace yakushima {

class tree_instance {
public:
    bool cas_root_ptr(base_node** expected, base_node** desired) {
        return weakCompareExchange(&root_, expected, desired);
    }

    bool empty() { return load_root_ptr() == nullptr; }

    base_node* load_root_ptr() { return loadAcquireN(root_); }

    void store_root_ptr(base_node* new_root) {
        return storeReleaseN(root_, new_root);
    }

private:
    base_node* root_{nullptr};
};

} // namespace yakushima