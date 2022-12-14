/**
 * @file permutation.h
 * @brief permutation which expresses the key order by index inside of the border node.
 */

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <bitset>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <tuple>
#include <vector>

#include <xmmintrin.h>

#include "base_node.h"
#include "log.h"
#include "scheme.h"

#include "glog/logging.h"

namespace yakushima {

class permutation {
public:
    /**
   * cnk ... current number of keys.
   */
    static constexpr std::size_t cnk_mask = 0b1111;
    static constexpr std::size_t cnk_bit_size = 4; // bits
    static constexpr std::size_t pkey_bit_size =
            4; // bits, permutation key size.

    permutation() : body_{} {}

    explicit permutation(const std::uint64_t body) : body_{body} {}

    /**
   * @brief decrement key number.
   */
    void dec_key_num() {
        std::uint64_t per_body(body_.load(std::memory_order_acquire));
        // decrement key number
        std::size_t cnk = per_body & cnk_mask;
        --cnk;
        per_body &= ~cnk_mask;
        per_body |= cnk;
        body_.store(per_body, std::memory_order_release);
    }

    void delete_rank(std::size_t rank) {
        // layout : left delete_target right cnk
        std::uint64_t left{};
        std::uint64_t cnk = get_cnk();
        if (rank == cnk - 1 || rank == key_slice_length - 1) {
            left = 0;
        } else {
            left = get_body();
            left = (left >> (pkey_bit_size * (rank + 2)))
                   << (pkey_bit_size * (rank + 1));
        }
        std::uint64_t right = get_body();
        if (rank == 0) {
            right = 0;
        } else {
            right = (right << (pkey_bit_size * (key_slice_length - rank))) >>
                    (pkey_bit_size * (key_slice_length - rank));
        }
        std::uint64_t final = left | right;
        final &= ~cnk_mask;
        final |= cnk;
        set_body(final);
    }

    void display() const {
        std::bitset<64> bs{get_body()};
        std::cout << " perm : " << bs << std::endl;
    }

    [[nodiscard]] std::size_t get_empty_slot() const {
        std::uint64_t per_body(body_.load(std::memory_order_acquire));
        std::size_t cnk = per_body & cnk_mask;
        if (cnk == 0) { return 0; }
        std::bitset<15> bs{};
        bs.reset();
        for (std::size_t i = 0; i < cnk; ++i) {
            per_body = per_body >> cnk_bit_size;
            bs.set(per_body & cnk_mask);
        }
        for (std::size_t i = 0; i < 15; ++i) {
            if (!bs.test(i)) { return i; }
        }
        LOG(ERROR) << log_location_prefix << "programming error";
        return 0;
    }

    [[nodiscard]] std::uint64_t get_body() const {
        return body_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::uint8_t get_cnk() const {
        std::uint64_t per_body(body_.load(std::memory_order_acquire));
        return static_cast<uint8_t>(per_body & cnk_mask);
    }

    [[nodiscard]] std::size_t get_lowest_key_pos() const {
        std::uint64_t per = get_body();
        per = per >> cnk_bit_size;
        return per & cnk_mask;
    }

    [[nodiscard]] std::size_t get_index_of_rank(const std::size_t rank) const {
        std::uint64_t per = get_body();
        per = per >> cnk_bit_size;
        if (rank != 0) { per = per >> (pkey_bit_size * rank); }
        return per & cnk_mask;
    }

    /**
   * @brief increment key number.
   */
    void inc_key_num() {
        std::uint64_t per_body(body_.load(std::memory_order_acquire));
        // increment key number
        std::size_t cnk = per_body & cnk_mask;
#ifndef NDEBUG
        if (cnk >= pow(2, cnk_bit_size) - 1) {
            LOG(ERROR) << log_location_prefix;
        }
#endif
        ++cnk;
        per_body &= ~cnk_mask;
        per_body |= cnk;
        body_.store(per_body, std::memory_order_release);
    }

    void init() { body_.store(0, std::memory_order_release); }

    void insert(std::size_t rank, std::size_t pos) {
        // bit layout : left target right cnk
        std::uint64_t cnk = get_cnk();
        std::uint64_t target = pos << (pkey_bit_size * (rank + 1));
        std::uint64_t left{};
        if (rank == cnk - 1) {
            left = 0;
        } else {
            left = get_body();
            left = (left >> (pkey_bit_size * (rank + 1)))
                   << (pkey_bit_size * (rank + 2));
        }
        std::uint64_t right{};
        if (rank == 0) {
            right = 0;
        } else {
            right = get_body();
            right = (right << (pkey_bit_size * (key_slice_length - rank))) >>
                    (pkey_bit_size * (key_slice_length - rank));
        }
        std::uint64_t final = left | target | right;
        final &= ~cnk_mask;
        final |= cnk;
        set_body(final);
    }

    /**
   * @brief for split
   */
    void split_dest(std::size_t num) {
        std::uint64_t body{0};
        for (std::size_t i = 1; i < num; ++i) {
            body |= i << (pkey_bit_size * (i + 1));
        }
        body |= num;
        set_body(body);
    }

    /**
   * @pre @a key_slice and @a key_length is array whose size is equal or less than cnk of
   * permutation. If it ignores, it may occur seg-v error.
   * @param key_slice
   * @param key_length
   */
    void
    rearrange(const std::array<key_slice_type, key_slice_length>& key_slice,
              const std::array<key_length_type, key_slice_length>& key_length) {
        std::uint64_t per_body(body_.load(std::memory_order_acquire));
        // get current number of keys
        auto cnk = static_cast<uint8_t>(per_body & cnk_mask);

        // tuple<key_slice, key_length, index>
        constexpr std::size_t key_pos = 1;
        std::array<std::tuple<base_node::key_tuple, std::uint8_t>,
                   key_slice_length>
                ar;
        for (std::uint8_t i = 0; i < cnk; ++i) {
            ar.at(i) = {{key_slice.at(i), key_length.at(i)}, i};
        }
        /**
     * sort based on key_slice and key_length for dictionary order.
     * So <key_slice_type, key_length_type, ...>
     */
        std::sort(ar.begin(), ar.begin() + cnk);

        // rearrange
        std::uint64_t new_body(0);
        for (auto itr = ar.rbegin() + (key_slice_length - cnk);
             itr != ar.rend(); ++itr) { // NOLINT : order to use "auto *itr"
            new_body |= std::get<key_pos>(*itr);
            new_body <<= pkey_bit_size;
        }
        new_body |= cnk;
        body_.store(new_body, std::memory_order_release);
    }

    void set_body(const std::uint64_t nb) {
        body_.store(nb, std::memory_order_release);
    }

    status set_cnk(const std::uint8_t cnk) {
#ifndef NDEBUG
        if (powl(2, cnk_bit_size) <= cnk) {
            LOG(ERROR) << log_location_prefix << "unreachable path";
        }
#endif
        std::uint64_t body = body_.load(std::memory_order_acquire);
        body &= ~cnk_mask;
        body |= cnk;
        body_.store(body, std::memory_order_release);
        return status::OK;
    }

private:
    std::atomic<std::uint64_t> body_;
};

} // namespace yakushima