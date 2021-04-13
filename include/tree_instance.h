#pragma once

#include "atomic_wrapper.h"
#include "base_node.h"

namespace yakushima {

class tree_instance {
    public:
    base_node* load_root_ptr() {
        return loadAcquireN(root_);
    }

    bool cas_root_ptr(base_node** expected, base_node** desired) {
        return weakCompareExchange(&root_, expected, desired);
    }
    private:
    base_node* root_{nullptr};
};

}