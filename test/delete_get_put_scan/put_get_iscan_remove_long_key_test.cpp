/**
 * @file
 * @brief check that the amount of code stack memory used by iscan functions when traversing a tree
 * does not depend on the tree's depth
 */

#include <array>
#include <mutex>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

std::string st{"1"}; // NOLINT

class put_get_iscan_remove_long_key_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "yakushima-test-delete_get_put_scan-put_get_iscan_remove_long_key_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        init();
        create_storage(st);
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

class pointer {
public:
    explicit pointer(char* p) : p_(p) {}
    std::size_t size() { return sizeof(p_); }
    char** data() { return &p_; }
    bool operator==(pointer o) { return p_ == o.p_; }
private:
    char* p_;
};

template<typename T>
struct ctx_cleaner{
    ctx_cleaner(T *&p) : ptr(p) {}
    ~ctx_cleaner(){if(ptr){delete ptr;}}
    T*& ptr;
};

TEST_F(put_get_iscan_remove_long_key_test, put_get) { // NOLINT
                                                     // prepare
    Token s{};
    ASSERT_EQ(status::OK, enter(s));
    std::string st{"test"};
    ASSERT_EQ(status::OK, create_storage(st));
    //std::size_t max_len = 2U * 1024U * 1024U; // 2Mi
    std::size_t max_len = 64U * 1024U; // 64Ki

    std::string v{"v"};
    for (std::size_t i = 1024; i <= max_len; i *= 2) { // NOLINT
        if (i >= 1024U*1024U) {
            LOG(INFO) << "test key size " << i << " B (" << i / (1024U*1024U) << " MiB)";
        } else {
            LOG(INFO) << "test key size " << i << " B (" << i / (1024U) << " KiB)";
        }
        std::string k(i, 'a');
        // test: put
        pointer p{v.data()};
        ASSERT_EQ(status::OK, put(s, st, k, p.data(), p.size()));

        // test: get
        std::pair<char*, std::size_t> tuple{};
        ASSERT_EQ(status::OK, get(st, k, tuple));

        // get verify
        //ASSERT_EQ(pointer(std::get<0>(tuple)), p);
        ASSERT_TRUE(pointer(std::get<0>(tuple)) == p);
        ASSERT_EQ(std::get<1>(tuple), p.size());

        // test and verify: iscan
        void* val;
        iscan_context* ctx{};
        ctx_cleaner<iscan_context> cleaner{ctx};
        ASSERT_EQ(status::OK, iscan_open(st, {}, scan_endpoint::INF, {}, scan_endpoint::INF, false, false, ctx, val));
        ASSERT_EQ(ctx->full_key(), k);
        //ASSERT_EQ(val, p);
        ASSERT_TRUE(val == v.data()) << "val:" << val << ", v.data():" << v.data();
        ASSERT_EQ(status::OK_SCAN_END, iscan_next(ctx, val));
        ASSERT_EQ(status::OK, iscan_close(ctx));

        // test: delete
        ASSERT_EQ(status::OK, remove(s, st, std::string_view(k)));

        // delete verify
        ASSERT_EQ(status::WARN_NOT_EXIST, get<char>(st, k, tuple));
    }

    // cleanup
    ASSERT_EQ(status::OK, leave(s));
}

} // namespace yakushima::testing
