/**
 * @file interface_display.h
 */

#pragma once

#include <sstream>

#include "border_node.h"
#include "interior_node.h"
#include "kvs.h"
#include "log.h"
#include "permutation.h"
#include "tree_instance.h"
#include "value.h"

#include "glog/logging.h"

namespace yakushima {

// forward declaration
static void display_node(std::stringstream& ss, base_node* n,
                         std::string& key_prefix);

static void display_border(std::stringstream& ss, border_node* n,
                           std::string& key_prefix) {
    ss << "border node, " << n << "\n";
    ss << "version_ptr " << n->get_version_ptr() << "\n";
    ss << "((key,key_len),value):";
    permutation perm{n->get_permutation().get_body()};
    for (std::size_t i = 0; i < perm.get_cnk(); ++i) {
        std::size_t index = perm.get_index_of_rank(i);
        value* value_ptr{n->get_lv_at(index)->get_value()};
        key_slice_type ks = n->get_key_slice_at(index);
        key_length_type kl = n->get_key_length_at(index);
        std::string key{};
        if (kl > 0) {
            if (kl > sizeof(key_slice_type)) {
                key = std::string(reinterpret_cast<char*>(&ks), // NOLINT
                                  sizeof(key_slice_type));
            } else {
                key = std::string(reinterpret_cast<char*>(&ks), kl); // NOLINT
            }
        }
        ss << "((" << key_prefix + key << ","
           << std::to_string(n->get_key_length_at(index) + key_prefix.size())
           << "),";
        if (kl > sizeof(key_slice_type)) {
            ss << n->get_lv_at(index)->get_next_layer();
        } else {
            std::string value_str{};
            void* val_ptr = value::get_body(value_ptr);
            auto val_len = value::get_len(value_ptr);
            if (val_len <= sizeof(void*)) { // inline opt
                if (val_len > 0) {
                    value_str = std::string(val_len, '0');
                    memcpy(value_str.data(), val_ptr, val_len);
                }
            } else { // not inline opt len > sizeof(void*)
                value_str =
                        std::string(reinterpret_cast<char*>(val_ptr), // NOLINT
                                    val_len);
            }
            ss << value_str;
        }
        ss << "), ";
    }
    ss << "\n";

    for (std::size_t i = 0; i < perm.get_cnk(); ++i) {
        std::size_t index = perm.get_index_of_rank(i);
        link_or_value* lv = n->get_lv_at(index);
        base_node* next_layer = lv->get_next_layer();
        if (n->get_key_length_at(index) > sizeof(key_slice_type)) {
            key_slice_type ks = n->get_key_slice_at(index);
            key_length_type kl = n->get_key_length_at(index);
            std::string key{};
            if (kl > 0) {
                if (kl > sizeof(key_slice_type)) {
                    key = std::string(reinterpret_cast<char*>(&ks), // NOLINT
                                      sizeof(key_slice_type));
                } else {
                    key = std::string(reinterpret_cast<char*>(&ks), // NOLINT
                                      kl);
                }
            }
            std::string new_prefix{key_prefix + key};
            display_node(ss, next_layer, new_prefix);
        }
    }
}

static void display_interior(std::stringstream& ss, interior_node* n,
                             std::string& key_prefix) {
    ss << "interior node, " << n << "\n";
    ss << "version_ptr " << n->get_version_ptr() << "\n";
    for (std::size_t i = 0; i < n->get_n_keys(); ++i) {
        key_slice_type ks = n->get_key_slice_at(i);
        key_length_type kl = n->get_key_length_at(i);
        std::string key{};
        if (kl > 0) {
            key = std::string(reinterpret_cast<char*>(&ks), kl); // NOLINT
        }
        ss << n->get_child_at(i) << "," << key_prefix + key << ",";
    }
    ss << n->get_child_at(n->get_n_keys()) << "\n";
    for (std::size_t i = 0; i <= n->get_n_keys(); ++i) {
        display_node(ss, n->get_child_at(i), key_prefix);
    }
}

static void display_node(std::stringstream& ss, base_node* n,
                         std::string& key_prefix) {
    auto* ver = n->get_version_ptr();
    auto verb = ver->get_stable_version();
    if (verb.get_border()) {
        display_border(ss, dynamic_cast<border_node*>(n), key_prefix);
    } else {
        display_interior(ss, dynamic_cast<interior_node*>(n), key_prefix);
    }
}

static void display_tree_instance(std::stringstream& ss, tree_instance* ti) {
    auto* n = ti->load_root_ptr();
    if (n == nullptr) {
        ss << "null\n";
        return;
    }
    std::string key_prefix{};
    display_node(ss, n, key_prefix);
}

// begin - forward declaration
[[maybe_unused]] static status list_storages(
        std::vector<std::pair<std::string, tree_instance*>>& out); // NOLINT
// end - forward declaration

[[maybe_unused]] static void display_body() {
    std::vector<std::pair<std::string, tree_instance*>> st_list{};

    auto rc = list_storages(st_list);

    if (rc != status::OK) { return; }
    std::stringstream ss;
    ss << log_location_prefix
       << "display() start. \nlist storages(key,tree_instance*): ";
    for (auto&& elem : st_list) {
        ss << "(" << elem.first << "," << elem.second << "), ";
    }
    ss << "\n";


    for (auto&& elem : st_list) {
        ss << "about (" << elem.first << "," << elem.second << ")\n";
        display_tree_instance(ss, elem.second);
    }

    LOG(INFO) << ss.str();
}

[[maybe_unused]] static void display() {
    yakushima_log_entry << "display";
    display_body();
    yakushima_log_exit << "display";
}

} // namespace yakushima
