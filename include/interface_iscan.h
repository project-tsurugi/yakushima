
#pragma once

#include <algorithm>
#include <iomanip>
#include <deque>

#include "kvs.h"
#include "log.h"
#include "storage.h"
#include "tree_instance.h"

#include "glog/logging.h"

namespace yakushima {

using key_tuple = border_node::key_tuple;

class iscan_context {
    // saved from iscan_open parameter
    tree_instance *ti_;
    std::string end_key_; // need padding 8bytes
    scan_endpoint end_point_;
    bool right_to_left_;
    bool early_abort_;

// NOLINTBEGIN(misc-non-private-member-variables-in-classes)
    // resume info
    struct bn_iterate_state {
        node_version64_body v_prev;
        permutation perm_prev; // not copyable, not movable
        std::size_t perm_rank; // need only 4bits

        bn_iterate_state(
            node_version64_body v_prev, permutation perm_prev, std::size_t perm_rank
        ) : v_prev(v_prev), perm_prev(perm_prev.get_body()), perm_rank(perm_rank) {}

        bn_iterate_state(const bn_iterate_state& bi)
        : v_prev(bi.v_prev), perm_prev(bi.perm_prev.get_body()), perm_rank(bi.perm_rank) {}
        bn_iterate_state& operator=(const bn_iterate_state& bi) {
            v_prev = bi.v_prev;
            perm_prev.set_body(bi.perm_prev.get_body());
            perm_rank = bi.perm_rank;
            return *this;
        }

        bn_iterate_state(bn_iterate_state&&) = delete;
        bn_iterate_state& operator=(bn_iterate_state&&) = delete;
        ~bn_iterate_state() = default;
    };
    struct stack_element {
        key_tuple key;
        base_node* layer_root;
        border_node* bn;
        int compare_to_end; // if non-zero, end is out of current layer
        bn_iterate_state bi;

        stack_element(key_tuple key, base_node* layer_root, border_node* bn, int cmp_end, const bn_iterate_state& bi)
            : key(key), layer_root(layer_root), bn(bn), compare_to_end(cmp_end), bi(bi) { }
    };
// NOLINTEND(misc-non-private-member-variables-in-classes)

    std::deque<stack_element> stackq_;

public:
    tree_instance *get_ti() { return ti_; }
    const std::string& get_end_key() { return end_key_; }
    scan_endpoint get_end_point() { return end_point_; }
    [[nodiscard]] bool get_right_to_left() const { return right_to_left_; }
    [[nodiscard]] bool get_early_abort() const { return early_abort_; }

    void stack(key_tuple kt, base_node* layer_root, border_node* bn, int cmp_end, const bn_iterate_state& bi) {
        VLOG(log_debug) << "stack [" << stackq_.size() << "] {kt:" << kt << ", Lroot:" << layer_root << ", bn:" << bn
                        << ", cmpend:" << cmp_end << ", bi{r" << bi.perm_rank << ",v{" << std::hex << bi.v_prev
                        << "},p" << std::hex << bi.perm_prev.get_body() << "}";
        stackq_.emplace_back(kt, layer_root, bn, cmp_end, bi);
    }
    stack_element& stack_top() {
        return stackq_.back();
    }
    void stack_pop() {
        VLOG(log_debug) << "stack_pop ssz:" << stackq_.size();
        return stackq_.pop_back();
    }
    [[nodiscard]] bool stack_empty() const { return stackq_.empty(); }
    [[nodiscard]] auto stack_size() const { return stackq_.size(); }
    void stack_clear() { stackq_.clear(); }

    std::string full_key() {
        std::string buf{};
        buf.reserve(stackq_.size() * sizeof(key_slice_type));
        for (auto&& elem : stackq_) {
            buf.append(reinterpret_cast<const char*>(&elem.key.get_key_slice()), // NOLINT
                       std::min<std::size_t>(elem.key.get_key_length(), sizeof(key_slice_type)));
        }
        return buf;
    }

    key_tuple get_end_tuple(int offset) {
        if (!right_to_left_ && end_point_ == scan_endpoint::INF) {
            return key_tuple::max(); // each slice of +inf
        }
        VLOG(log_debug) << "get_end_tuple: ofs:" << offset << ", ssz:" << stack_size() << ", {\"" << get_end_key() << "\"," << get_end_key().size() << "}";
        return key_tuple(std::string_view(get_end_key()).substr((stack_size() + offset) * sizeof(key_slice_type)));
    }

    iscan_context(
        tree_instance *ti,
        std::string_view end_key_sv, // from string_view
        scan_endpoint end_point,
        bool right_to_left,
        bool early_abort
    ) : ti_(ti), end_point_(end_point), right_to_left_(right_to_left), early_abort_(early_abort) {
        end_key_.reserve((end_key_sv.size() + 7) & ~7U); // round up
        end_key_.assign(end_key_sv);
    }
};

inline status iscan_check_retry(border_node* const bn, node_version64_body& v_at_fb, permutation& perm) {
    node_version64_body check_v = bn->get_stable_version();
    std::uint64_t check_perm_b{bn->get_permutation().get_body()};
    for (auto recheck = bn->get_stable_version(); recheck != check_v; ) {
        check_v = recheck;
        check_perm_b = bn->get_permutation().get_body();
    }
    if (check_v != v_at_fb || check_perm_b != perm.get_body()) {
        // fail optimistic verify
        VLOG(log_debug) << "scan_check_retry fail check_v,perm:\n" << check_v << "," << std::hex << check_perm_b
                        << "\nv_at_fb,perm\n" << std::dec << v_at_fb << "," << std::hex << perm.get_body();
        if (check_v.get_vsplit() != v_at_fb.get_vsplit() || check_v.get_deleted()) {
            // The node at find border was changed by split or deleted.
            return status::OK_RETRY_FROM_ROOT;
        }
        // The structure of the border node was not changed.
        // So reading border node can retry from that.
        v_at_fb = check_v;
        perm.set_body(check_perm_b);
        return status::OK_RETRY_AFTER_FB;
    }
    return status::OK;
}

// find first key location
// OK -> found key/value
// SCAN_CONTINUE -> found key/value but skip this (endpoint type = EXCLUSIVE)
// WARN_CONCURRENT_OPERATIONS -> detected concurrent modification  // XXX: use another status code
// WARN_ABORTED_BY_USER -> aborted by user
static status
iscan_findfirst(iscan_context* ctx, std::string_view start_key, scan_endpoint start_point, void *&out,
                const std::function<bool(node_version64*, node_version64_body)>& bnv_cb) {
    bool right_to_left = ctx->get_right_to_left();
    bool early_abort = ctx->get_early_abort();
    bool range_is_one_point = start_key == ctx->get_end_key()
                              && start_point == scan_endpoint::INCLUSIVE
                              && ctx->get_end_point() == scan_endpoint::INCLUSIVE;

retry_from_root: // retry from Masstree root
    base_node* root = ctx->get_ti()->load_root_ptr();
    VLOG(log_debug) << "FF root: " << root;
    if (root == nullptr) {
        // no border to callback is exist, so give up callback
        return status::OK_SCAN_END;
    }

    std::string_view traverse_key_view{start_key};
    scan_endpoint traverse_endpoint{start_point};
    int cmp_to_end = 0;
    if (!right_to_left && traverse_endpoint == scan_endpoint::INF) {
        cmp_to_end = -1; // any point in layer-0 <= INF
    }

//retry_find_border:
next_layer:
    // prepare key_tuple
    key_tuple key_tup{};
    if (right_to_left && traverse_endpoint == scan_endpoint::INF) {
        // put maximum value of key_slice
        key_tup = key_tuple::max();
    } else {
        key_tup = key_tuple{traverse_key_view};
    }

    // traverse tree to border node.
    status check_status{status::OK};
    std::tuple<border_node*, node_version64_body> node_and_v =
            find_border(root, key_tup.get_key_slice(), key_tup.get_key_length(), check_status);
    if (check_status == status::WARN_RETRY_FROM_ROOT_OF_ALL) {
        // the root node of the some layer was deleted.
        // So it must retry from root of the all tree.
        if (early_abort) { return status::WARN_CONCURRENT_OPERATIONS; }
        goto retry_from_root; // NOLINT
    }
    constexpr std::size_t tuple_node_index = 0;
    constexpr std::size_t tuple_v_index = 1;
    border_node* target_border = std::get<tuple_node_index>(node_and_v);
    node_version64_body v_at_fb = std::get<tuple_v_index>(node_and_v);
    VLOG(log_debug) << "v_at_fb: " << v_at_fb;
    if (v_at_fb.get_deleted() && v_at_fb.get_root()) {
        // empty masstree
        if (bnv_cb(target_border->get_version_ptr(), v_at_fb)) { return status::WARN_ABORTED_BY_USER; }
        VLOG(log_debug) << "FF deleted root SCAN END";
        return status::OK_SCAN_END;
    }

retry_fetch_lv:
    node_version64_body v_at_fetch_lv{};
    std::size_t lv_pos{0}; // index, not rank
    link_or_value* lv_ptr = target_border->get_lv_of(
            key_tup.get_key_slice(), key_tup.get_key_length(), v_at_fetch_lv, lv_pos);

    // check
    if ((v_at_fetch_lv.get_vsplit() != v_at_fb.get_vsplit()) ||
        (v_at_fetch_lv.get_deleted() && !v_at_fetch_lv.get_root())) {
        /*
         * It may be change the correct border between atomically fetching border node and
         * atomically fetching lv.
         */
        goto retry_from_root; // NOLINT
    }
    if (lv_ptr != nullptr && target_border->get_key_length_at(lv_pos) > sizeof(key_slice_type)) {
        // case 1. lv_ptr != nullptr, and link to next-layer
        // visited this node

        root = lv_ptr->get_next_layer();
        if (root == nullptr) {
            if (early_abort) { return status::WARN_CONCURRENT_OPERATIONS; }
            goto retry_fetch_lv; // NOLINT
        }

        // recheck border version
        node_version64_body final_check = target_border->get_stable_version();
        if ((final_check.get_deleted() && !final_check.get_root()) || // this border was deleted.
            final_check.get_vsplit() != v_at_fb.get_vsplit()) { // this border may be incorrect.
            if (early_abort) { return status::WARN_CONCURRENT_OPERATIONS; }
            goto retry_from_root; // NOLINT
        }
        // check whether fetching lv is still correct.
        if (final_check.get_vinsert_delete() != v_at_fetch_lv.get_vinsert_delete()) { // fetched lv may be deleted
            if (early_abort) { return status::WARN_CONCURRENT_OPERATIONS; }
            goto retry_fetch_lv; // NOLINT
        }
        // root was fetched correctly.
        // root = lv; advance key; goto retry_find_border;
        traverse_key_view.remove_prefix(sizeof(key_slice_type));
        ctx->stack(key_tup, root, target_border, cmp_to_end,
                   {v_at_fb, permutation(target_border->get_permutation().get_body()), 0});
        if (cmp_to_end == 0) {
            VLOG(log_debug) << key_tup << "/" << ctx->get_end_tuple(-1);
            if (key_tup != ctx->get_end_tuple(-1)) {
                cmp_to_end = -1;
            }
        }
        VLOG(log_debug) << "FF next_layer";
        goto next_layer; // NOLINT
    }

    if (lv_ptr != nullptr) {
        // case 2. lv_ptr != nullptr, and it is value
        // just hit start_key
        if (traverse_endpoint == scan_endpoint::INCLUSIVE) {
            // not visit the border, so not call cb
            value* vp = lv_ptr->get_value();
            auto* v_body = value::get_body(vp);
            node_version64_body final_check = target_border->get_stable_version();
            if (final_check.get_vsplit() != v_at_fb.get_vsplit() ||
                (final_check.get_deleted() && !final_check.get_root())) {
                goto retry_from_root; // NOLINT
            }
            if (final_check.get_vinsert_delete() != v_at_fetch_lv.get_vinsert_delete()) {
                goto retry_fetch_lv; // NOLINT
            }
            out = v_body;
            ctx->stack(key_tup, root, target_border, cmp_to_end,
                       {v_at_fb, permutation(target_border->get_permutation().get_body()), 0});
            VLOG(log_debug) << "FF case2 OK key=" << ctx->full_key();
            return status::OK;
        }
        // pass to findnext
        ctx->stack(key_tup, root, target_border, cmp_to_end,
                   {v_at_fb, permutation(target_border->get_permutation().get_body()), 0});
        VLOG(log_debug) << "FF case2 CONTINUE";
        return status::OK_SCAN_CONTINUE;
    } else { // NOLINT
        // case 3. lv_ptr == nullptr
        // this border is first node, but the key does not exist here

        // skip callback. will called in findnext
        // expception: if start=end, findnext does not call cb, so need cb here
        if (range_is_one_point) {
            if (bnv_cb(target_border->get_version_ptr(), v_at_fetch_lv)) { return status::WARN_ABORTED_BY_USER; }
        }

        ctx->stack(key_tup, root, target_border, cmp_to_end,
                   {v_at_fb, permutation(target_border->get_permutation().get_body()), 0});
        VLOG(log_debug) << "FF case3 CONTINUE";
        return status::OK_SCAN_CONTINUE; // pass to findnext
    }
}

static status iscan_next(iscan_context*, void*&, const std::function<bool(node_version64*, node_version64_body)>&);

static status
iscan_open(tree_instance* ti, std::string_view l_key, scan_endpoint l_end, std::string_view r_key, scan_endpoint r_end,
           iscan_context*& context,
           void *&out,
           const std::function<bool(node_version64*, node_version64_body)>& bnv_cb,
           bool right_to_left, bool early_abort) {
    auto* ctx = new iscan_context( // NOLINT
        ti, right_to_left ? l_key : r_key, right_to_left ? l_end : r_end,
        right_to_left, early_abort);
    context = ctx;

    auto rc = iscan_findfirst(ctx, right_to_left ? r_key : l_key, right_to_left ? r_end : l_end, out, bnv_cb);
    if (rc != status::OK_SCAN_CONTINUE) { return rc; }
    return iscan_next(ctx, out, bnv_cb);
}

// find next key/value
// returns
// OK : found key/value. stored value to `out`
// OK_SCAN_END : reach end_key, return to upper layer
// OK_SCAN_CONTINUE : done iterating border nodes in this layer, return to upper layer
// OK_RETRY_FROM_ROOT : retry from layer root
static status
iscan_findnext(iscan_context* ctx,
    void *&out,
    const std::function<bool(node_version64*, node_version64_body)>& bnv_cb
) {
    bool right_to_left = ctx->get_right_to_left();
    bool early_abort = ctx->get_early_abort();

next_layer:
    key_tuple last_key = ctx->stack_top().key;
    auto* st = &ctx->stack_top(); // alias to stack top

    auto cmp_to_end = st->compare_to_end;

    if (false) { // NOLINT(*-simplify-boolean-expr)
retry_from_root:
        base_node* root = ctx->stack_top().layer_root;
        auto rv = root->get_stable_version();
        VLOG(log_debug) << "FN retry_from_root: root:" << root << ", v{" << rv << "}";
        if (rv.get_deleted()) {
            // border in upper layer is modified
            // return to upper root
            if (ctx->stack_size() == 1) { // L0
                base_node* new_mt_root = ctx->get_ti()->load_root_ptr();
                if (root != new_mt_root) {
                    ctx->stack_top().layer_root = new_mt_root;
                    goto retry_from_root; // NOLINT
                }
                // mt root is deleted, so scan end
                VLOG(log_debug) << "FN mt root is deleted. root:" << root << ", v{" << rv << "}";
                return status::OK_SCAN_END;
            }
            // L1+
            ctx->stack_pop();
            st = &ctx->stack_top(); // sync alias
            goto retry_from_root; // NOLINT
        }
        if (!rv.get_root()) {
            // saved-root is now not root. split?
            // return to border in upper layer
            if (ctx->stack_size() == 1) { // L0
                base_node* new_mt_root = ctx->get_ti()->load_root_ptr();
                ctx->stack_top().layer_root = new_mt_root;
                goto retry_from_root; // NOLINT
            }
            ctx->stack_pop();
            st = &ctx->stack_top(); // sync alias
            goto next_layer; // NOLINT // or jump to entry point of this function
        }
        status check_status{};
        auto border_node_and_v =
            find_border(root, last_key.get_key_slice(), last_key.get_key_length(), check_status);
        border_node* target_border = std::get<0>(border_node_and_v);
        if (check_status != status::OK) {
            VLOG(log_debug) << "FN retry_from_root FB=" << check_status;
            goto retry_from_root; // NOLINT
        }
        ctx->stack_top().bn = target_border;
        ctx->stack_top().bi.perm_rank = 0;
        ctx->stack_top().bi.perm_prev.set_body(target_border->get_permutation().get_body());
        ctx->stack_top().bi.v_prev = std::get<1>(border_node_and_v);
    }
    border_node* bn = st->bn;
    node_version64_body v_at_fb = st->bi.v_prev;
    permutation perm(st->bi.perm_prev.get_body());

from_neighbor:
    if (false) { // NOLINT(*-simplify-boolean-expr)
retry_after_fb:
        // check index rewinding in this border is sufficient
        if (perm.get_cnk() == 0) { goto retry_from_root; } // NOLINT
        if (!right_to_left) {
            // check min keyslice
            std::size_t index = perm.get_index_of_rank(0);
            auto kt = key_tuple(bn->get_key_slice_at(index), bn->get_key_length_at(index));
            if (kt > last_key) {
                goto retry_from_root; // NOLINT
            }
        } else {
            // check max keyslice
            std::size_t index = perm.get_index_of_rank(perm.get_cnk() - 1);
            auto kt = key_tuple(bn->get_key_slice_at(index), bn->get_key_length_at(index));
            if (kt < last_key) {
                goto retry_from_root; // NOLINT
            }
        }
        // optimistic-check for kt
        status check_status = iscan_check_retry(bn, v_at_fb, perm);
        if (check_status != status::OK) { goto retry_from_root; } // NOLINT

        st->bi.perm_rank = 0;
        st->bi.perm_prev.set_body(perm.get_body());
    }
//retry:
    // in scan_helper::scan_border comment:
    // "next node pointer must be logged before optimistic verify."
    border_node* to_bn = right_to_left ? bn->get_prev() : bn->get_next();
    // TODO check at resume, version

    std::size_t i = st->bi.perm_rank;
    VLOG(log_debug) << "FN ctx:" << ctx << " top=[" << ctx->stack_size()-1 << "]:{Lroot:" << st->layer_root
                    << " bn:" << bn << ", lastkey:" << last_key << ", cmpend:" << cmp_to_end << ", bi{r" << i
                    << ",v{" << st->bi.v_prev << "},p" << std::hex << st->bi.perm_prev.get_body() << "}}";
    // check all elements in this border node.
    auto ekt = cmp_to_end == 0 ? ctx->get_end_tuple(-1) : (right_to_left ? key_tuple::min() : key_tuple::max());
    auto eep = ctx->get_end_point();
    for (std::size_t n = perm.get_cnk(); i < n; ++i) {
        std::size_t index = perm.get_index_of_rank(right_to_left ? n-i-1 : i);
        key_slice_type ks = bn->get_key_slice_at(index);
        key_length_type kl = bn->get_key_length_at(index);
        auto kt = key_tuple(ks, kl);

        link_or_value* lv = bn->get_lv_at(index);
        value* vp = lv->get_value();
        // base_node* next_layer = lv->get_next_layer();

        /*
         * This verification may seem verbose, but it can also be considered
         * an early abort.
         */
        status check_status = iscan_check_retry(bn, v_at_fb, perm);
        if (check_status != status::OK) {
            if (early_abort) { return status::WARN_CONCURRENT_OPERATIONS; }
            if (check_status == status::OK_RETRY_FROM_ROOT) {
                VLOG(log_debug) << "FN retry FROM_ROOT 1";
                goto retry_from_root; // NOLINT
            }
            if (check_status == status::OK_RETRY_AFTER_FB) {
                VLOG(log_debug) << "FN retry AFTER_FB 1";
                goto retry_after_fb; // NOLINT
            }
        }
        // start-side check
        bool hit{};
        VLOG(log_debug) << "FN start-side-check: " << last_key << "/" << kt << " EXCLUSIVE"
                        << (right_to_left ? " (rev)" : "");
        if (!right_to_left) { // forward order
            hit = (last_key < kt);
        } else { // reverse order
            hit = (last_key > kt);
        }
        VLOG(log_debug) << "FN start-side-check: " << std::boolalpha << hit;
        if (!hit) { continue; } // skip this key/value

        // end-side check
        if (cmp_to_end == 0) {
            VLOG(log_debug) << "FN end-side-check: " << kt << "/" << ekt << " " << eep
                            << (right_to_left ? " (rev)" : "");
            if (!right_to_left) { // forward order
                if (eep == scan_endpoint::INCLUSIVE) {
                    hit = !(kt > ekt);
                } else {
                    hit = (kt < ekt) || (kt == ekt && kt.get_key_length() > sizeof(key_slice_type));
                }
            } else { // reverse order
                if (eep == scan_endpoint::INCLUSIVE) {
                    hit = !(kt < ekt);
                } else {
                    hit = (kt > ekt) || (kt == ekt && kt.get_key_length() > sizeof(key_slice_type));
                }
            }
            if (!hit) { // reach to range end
                VLOG(log_debug) << "FN end-side-check: false (range-check)";
                // callback range, from last_key to range_end.
                VLOG(log_debug) << "FN end-side cb-check: eep:" << eep << ", " << last_key << "/" << ekt;
                // if last_key = range_end_key and range_end_ep = INCLUSIVE, callback range is empty
                if (!(eep == scan_endpoint::INCLUSIVE && last_key == ekt)) { // NOLINT(*-simplify-boolean-expr)
                    if (bnv_cb(bn->get_version_ptr(), v_at_fb)) {
                        return status::WARN_ABORTED_BY_USER;
                    }
                }
                return status::OK_SCAN_END;
            }
            VLOG(log_debug) << "FN end-side-check: true (range-check pass)";
        } else {
            VLOG(log_debug) << "FN end-side-check: true (all of this layer is in-range)";
        }
        // in range
        if (kl > sizeof(key_slice_type)) {
            base_node* child = lv->get_next_layer();
            if (child == nullptr) {
                if (early_abort) { return status::WARN_CONCURRENT_OPERATIONS; }
//                goto retry_fetch_lv; // NOLINT
            }
            // TODO: implement check and retry

            if (bnv_cb(bn->get_version_ptr(), v_at_fb)) {
                return status::WARN_ABORTED_BY_USER;
            }
            key_tuple child_kt = right_to_left ? key_tuple::max() : key_tuple::min();
            auto child_border_node_and_v =
                find_border(child, child_kt.get_key_slice(), child_kt.get_key_length(), check_status);
            border_node* target_border = std::get<0>(child_border_node_and_v);
            // save stack context
            ctx->stack_top().bn = bn;
            ctx->stack_top().key = kt;
            ctx->stack_top().bi.perm_rank = i+1;
            // create context of next start
            if (cmp_to_end == 0) {
                if (kt != ekt) {
                    cmp_to_end = -1;
                }
            }
            ctx->stack(child_kt, child, target_border, cmp_to_end,
                       {std::get<1>(child_border_node_and_v),
                        permutation(target_border->get_permutation().get_body()), 0});
            VLOG(log_debug) << "FN next_layer";
            goto next_layer; // NOLINT
        } else {
            // hit value
            auto* v_body = value::get_body(vp);

            // final check for atomicity
            status check_status = iscan_check_retry(bn, v_at_fb, perm);
            if (check_status != status::OK) {
                if (early_abort) { return status::WARN_CONCURRENT_OPERATIONS; }
                if (check_status == status::OK_RETRY_FROM_ROOT) {
                    VLOG(log_debug) << "FN retry FROM_ROOT hit-valuw";
                    goto retry_from_root; // NOLINT
                }
                if (check_status == status::OK_RETRY_AFTER_FB) {
                    VLOG(log_debug) << "FN retry AFTER_FB hit-valuw";
                    goto retry_after_fb; // NOLINT
                }
            }
            if (bnv_cb(bn->get_version_ptr(), v_at_fb)) {
                return status::WARN_ABORTED_BY_USER;
            }
            //out = std::make_pair(v_body, value::get_len(vp));
            out = v_body;
            ctx->stack_top().bn = bn;
            ctx->stack_top().key = {ks, kl};
            ctx->stack_top().bi.perm_rank = i+1;
            VLOG(log_debug) << "FN OK: key=" << ctx->full_key();
            return status::OK;
        }
    }
    VLOG(log_debug) << "FN permutation iterate done bn:" << bn;
    // permutation iteration done
    // move to neighbour

    // log before verify for atomicity
    node_version64_body to_version{};
    std::uint64_t to_perm_body{};
    {
        border_node* check_to_bn = right_to_left ? bn->get_prev() : bn->get_next();
        if (to_bn != check_to_bn) {
            VLOG(log_debug) << "FN retry_from_root: neighbor node is changed";
            goto retry_from_root; // NOLINT
        }
    }
    if (to_bn != nullptr) {
        to_version = to_bn->get_stable_version();
        if (to_version.get_deleted()) { // XXX
            VLOG(log_debug) << "FN to_bn is deleted: " << to_bn;
            goto retry_from_root; // NOLINT
        }
        to_perm_body = to_bn->get_permutation().get_body();
    }

    // final check for atomicity
    status check_status = iscan_check_retry(bn, v_at_fb, perm);
    if (check_status != status::OK) {
        if (early_abort) { return status::WARN_CONCURRENT_OPERATIONS; }
        if (check_status == status::OK_RETRY_FROM_ROOT) {
            VLOG(log_debug) << "FN retry FROM_ROOT perm";
            goto retry_from_root; // NOLINT
        }
        if (check_status == status::OK_RETRY_AFTER_FB) {
            VLOG(log_debug) << "FN retry AFTER_FB perm-end";
            goto retry_after_fb; // NOLINT
        }
    }

    // callback range, from last_key to range_end.
    // if last_key = range_end_key and range_end_ep = INCLUSIVE, callback range is empty
    if (!(eep == scan_endpoint::INCLUSIVE && last_key == ekt)) { // NOLINT(*-simplify-boolean-expr)
        if (bnv_cb(bn->get_version_ptr(), v_at_fb)) {
            return status::WARN_ABORTED_BY_USER;
        }
    }

    // reaches endpoint of entire layer-b+tree.
    if (to_bn == nullptr) {
        // end-side check
        if (cmp_to_end == 0) {
            VLOG(log_debug) << "FN end-side-check: false";
            // end-point is before inf (= entire-b+tree), so it might be reached end-point
            return status::OK_SCAN_END;
        }
        VLOG(log_debug) << "FN end-side-check: true (all of this layer is in-range)";
        VLOG(log_debug) << "FN CONTINUE";
        return status::OK_SCAN_CONTINUE;
    }
    if ((right_to_left ? to_bn->get_next() : to_bn->get_prev()) != bn) {
        VLOG(log_debug) << "FN NEIGHBOR dual-link-check failed, retry from root";
        goto retry_from_root; // NOLINT
    }

    // it is in scan range and fin scaning this border node.
    VLOG(log_debug) << "FN NEIGHBOR move from " << bn << " to " << to_bn;
    bn = to_bn;
    v_at_fb = to_version;
    perm.set_body(to_perm_body);
    ctx->stack_top().bn = bn;
    ctx->stack_top().bi.perm_rank = 0;
    ctx->stack_top().bi.v_prev = to_version;
    ctx->stack_top().bi.perm_prev.set_body(to_perm_body);
    goto from_neighbor; // NOLINT
}

const static auto dummycallback = [](node_version64*, node_version64_body){ return false; };

//
// API
//

/**
 * @brief open scan iterator and find first key
 * @return status::OK if found first key, and stored value to @a value and stored scan context to @a context.
 * @return status::OK_SCAN_END if not found any key in range.
 * @return status::ERR_BAD_USAGE if given invalid parameter.
 * @return status::WARN_STORAGE_NOT_EXIST if storage does not exist.
 * @return status::WARN_ABORTED_BY_USER if aborted by user callback.
 * @return status::WARN_CONCURRENT_OPERATIONS if detected concurrent modification.
 */
[[maybe_unused]] static auto
iscan_open(std::string_view storage_name,
        std::string_view l_key, scan_endpoint l_end,
        std::string_view r_key, scan_endpoint r_end,
        bool right_to_left, bool early_abort,
        iscan_context*& context, void*& value,
        const std::function<bool(node_version64*, node_version64_body)>& bnv_cb = dummycallback) {
    if ((l_key.data() == nullptr && !l_key.empty()) ||
        (r_key.data() == nullptr && !r_key.empty())) {
        context = nullptr;
        return status::ERR_BAD_USAGE;
    }

    if (auto rc = check_empty_scan_range(l_key, l_end, r_key, r_end); rc != status::OK) {
        context = nullptr;
        return rc;
    }

    // check storage
    tree_instance* ti{};
    if (storage::find_storage(storage_name, &ti) != status::OK) {
        context = nullptr;
        return status::WARN_STORAGE_NOT_EXIST;
    }
    if (l_end == scan_endpoint::INF) {
        // treat l_key as ""
        l_key = "";
        l_end = scan_endpoint::INCLUSIVE;
    }
    return iscan_open(ti, l_key, l_end, r_key, r_end, context, value, bnv_cb, right_to_left, early_abort);
}

/**
 * @brief find next key using scan context
 * @return status::OK if found first key, and stored value to @a value.
 * @return status::OK_SCAN_END if not found any key in range.
 * @return status::WARN_ABORTED_BY_USER if aborted by user callback.
 * @return status::WARN_CONCURRENT_OPERATIONS if detected concurrent modification.
 */
[[maybe_unused]] static status
iscan_next(iscan_context* ctx, void*& value,
        const std::function<bool(node_version64*, node_version64_body)>& bnv_cb = dummycallback) {
    while (true) {
        auto rc = iscan_findnext(ctx, value, bnv_cb);
        VLOG_IF(log_debug, rc == status::OK) << rc << " value:" << value;
        VLOG_IF(log_debug, rc != status::OK) << rc;
        if (rc == status::OK_SCAN_END) {
            ctx->stack_clear();
            return rc;
        }
        if (rc == status::OK) {
            return rc; // return value
        }
        if (rc == status::WARN_CONCURRENT_OPERATIONS || rc == status::WARN_ABORTED_BY_USER) {
            return rc;
        }
        if (rc == status::OK_SCAN_CONTINUE) { // TODO: stop exiting and re-entering iscan_findnext
            // layer end
            // return upto
            ctx->stack_pop();
            if (ctx->stack_empty()) { return status::OK_SCAN_END; }
            continue;
        }
        break;
    }
    VLOG(log_debug) << "unimplemented";
    return status::ERR_FATAL;
}

/**
 * @brief close iterator and dispose scan context
 * @return status::OK
 */
[[maybe_unused]] static auto
iscan_close(iscan_context*& context) {
    delete context; // NOLINT
    context = nullptr;
    return status::OK;
}

} // namespace yakushima
