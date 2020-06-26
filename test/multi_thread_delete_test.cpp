/**
 * @file multi_thread_delete_test.cpp
 */

#include <algorithm>
#include <future>
#include <random>
#include <thread>
#include <tuple>

#include "gtest/gtest.h"

#include "kvs.h"

#include "debug.hh"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class multi_thread_delete_test : public ::testing::Test {
protected:
  multi_thread_delete_test() {}

  ~multi_thread_delete_test() {}
};

TEST_F(multi_thread_delete_test, test1) {
  /**
   * Initial state : multi threads put same null char key slices and different key length to multiple border.
   * Concurrent remove against initial state.
   */

  constexpr std::size_t ary_size = 9;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < 5; ++i) {
    kv1.emplace_back(std::string(i, '\0'), std::to_string(i));
  }
  for (std::size_t i = 5; i < ary_size; ++i) {
    kv2.emplace_back(std::string(i, '\0'), std::to_string(i));
  }

#ifndef NDEBUG
  for (std::size_t h = 0; h < 5; ++h) {
#else
  for (std::size_t h = 0; h < 100; ++h) {
#endif
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);
    struct S {
      static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            EXPECT_EQ(ret, status::OK); // output log
            std::abort();
          }
        }
      }

      static void remove_work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::remove(token, std::string_view(k));
          if (ret != status::OK) {
            EXPECT_EQ(ret, status::OK); // output log
            std::abort();
          }
        }
      }

    };

    std::thread t(S::put_work, std::ref(kv2));
    S::put_work(std::ref(kv1));
    t.join();

    t = std::thread(S::remove_work, std::ref(kv2), std::ref(token[0]));
    S::remove_work(std::ref(kv1), std::ref(token[1]));
    t.join();

    ASSERT_EQ(masstree_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::leave(token[1]), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(multi_thread_delete_test, test2) {
  /**
   * Initial state : multi threads put same null char key slices and different key length to multiple border, which
   * is using shuffled data.
   * Concurrent remove against initial state.
   */

  constexpr std::size_t ary_size = 9;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < 5; ++i) {
    kv1.emplace_back(std::string(i, '\0'), std::to_string(i));
  }
  for (std::size_t i = 5; i < ary_size; ++i) {
    kv2.emplace_back(std::string(i, '\0'), std::to_string(i));
  }
  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());
#ifndef NDEBUG
  for (std::size_t h = 0; h < 5; ++h) {
#else
  for (std::size_t h = 0; h < 100; ++h) {
#endif
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }

      static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::remove(token, std::string_view(k));
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }
    };

    std::thread t(S::put_work, std::ref(kv2));
    S::put_work(std::ref(kv1));
    t.join();

    t = std::thread(S::remove_work, std::ref(token[0]), std::ref(kv2));
    S::remove_work(std::ref(token[1]), std::ref(kv1));
    t.join();

    ASSERT_EQ(masstree_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::leave(token[1]), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(multi_thread_delete_test, test3) {
  /**
   * Initial state : multi threads put same null char key slices and different key length to single border.
   * Concurrent remove against initial state.
   */

  constexpr std::size_t ary_size = 15;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < 7; ++i) {
    kv1.emplace_back(std::string(i, '\0'), std::to_string(i));
  }
  for (std::size_t i = 7; i < ary_size; ++i) {
    kv2.emplace_back(std::string(i, '\0'), std::to_string(i));
  }

#ifndef NDEBUG
  for (std::size_t h = 0; h < 5; ++h) {
#else
  for (std::size_t h = 0; h < 200; ++h) {
#endif
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);

    struct S {
      static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }

      static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::remove(token, std::string_view(k));
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }
    };

    std::thread t(S::put_work, std::ref(kv2));
    S::put_work(std::ref(kv1));
    t.join();

    t = std::thread(S::remove_work, std::ref(token[0]), std::ref(kv1));
    S::remove_work(std::ref(token[1]), std::ref(kv2));
    t.join();

    ASSERT_EQ(masstree_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::leave(token[1]), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(multi_thread_delete_test, test4) {
  /**
   * Initial state : multi threads put same null char key slices and different key length to single border, which
   * is using shuffled data.
   * Concurrent remove against initial state.
   */

  constexpr std::size_t ary_size = 15;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::string(i, '\0'), std::to_string(i));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::string(i, '\0'), std::to_string(i));
  }
  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 5; ++h) {
#else
  for (std::size_t h = 0; h < 100; ++h) {
#endif
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }

      static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::remove(token, std::string_view(k));
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }
    };

    std::thread t(S::put_work, std::ref(kv1));
    S::put_work(std::ref(kv2));
    t.join();

    t = std::thread(S::remove_work, std::ref(token[0]), std::ref(kv1));
    S::remove_work(std::ref(token[1]), std::ref(kv2));
    t.join();

    ASSERT_EQ(masstree_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::leave(token[1]), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(multi_thread_delete_test, test5) {
  /**
   * Initial state : multi threads put until first split of border.
   * Concurrent remove against initial state.
   */

  constexpr std::size_t ary_size = base_node::key_slice_length + 1;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::string(1, i), std::to_string(i));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::string(1, i), std::to_string(i));
  }

#ifndef NDEBUG
  for (std::size_t h = 0; h < 5; ++h) {
#else
  for (std::size_t h = 0; h < 100; ++h) {
#endif
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);

    struct S {
      static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }

      static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::remove(token, std::string_view(k));
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }
    };

    std::thread t(S::put_work, std::ref(kv1));
    S::put_work(std::ref(kv2));
    t.join();

    t = std::thread(S::remove_work, std::ref(token[0]), std::ref(kv1));
    S::remove_work(std::ref(token[1]), std::ref(kv2));
    t.join();

    ASSERT_EQ(masstree_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::leave(token[1]), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(multi_thread_delete_test, test6) {
  /**
   * Initial state : multi threads put until first split of border, which is using shuffled data.
   * Concurrent remove against initial state.
   */

  constexpr std::size_t ary_size = base_node::key_slice_length + 1;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::string(1, i), std::to_string(i));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::string(1, i), std::to_string(i));
  }
  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 5; ++h) {
#else
  for (std::size_t h = 0; h < 100; ++h) {
#endif
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }

      static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::remove(token, std::string_view(k));
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }
    };

    std::thread t(S::put_work, std::ref(kv1));
    S::put_work(std::ref(kv2));
    t.join();

    t = std::thread(S::remove_work, std::ref(token[0]), std::ref(kv1));
    S::remove_work(std::ref(token[1]), std::ref(kv2));
    t.join();

    ASSERT_EQ(masstree_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::leave(token[1]), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(multi_thread_delete_test, test7) {
  /**
   * Initial state : multi threads put between first split of border and first split of interior.
   * Concurrent remove against initial state.
   */

  constexpr std::size_t ary_size = 100;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::string(1, i), std::to_string(i));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::string(1, i), std::to_string(i));
  }

#ifndef NDEBUG
  for (size_t h = 0; h < 5; ++h) {
#else
  for (size_t h = 0; h < 20; ++h) {
#endif
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);

    struct S {
      static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }

      static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::remove(token, std::string_view(k));
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }
    };

    std::thread t(S::put_work, std::ref(kv1));
    S::put_work(std::ref(kv2));
    t.join();

    t = std::thread(S::remove_work, std::ref(token[0]), std::ref(kv1));
    S::remove_work(std::ref(token[1]), std::ref(kv2));
    t.join();

    ASSERT_EQ(masstree_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::leave(token[1]), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(multi_thread_delete_test, test8) {
  /**
   * Initial state : multi threads put between first split of border and first split of interior, which is using
   * shuffled data.
   * Concurrent remove against initial state.
   */

  constexpr std::size_t ary_size = 100;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::string(1, i), std::to_string(i));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::string(1, i), std::to_string(i));
  }

  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 5; ++h) {
#else
  for (std::size_t h = 0; h < 30; ++h) {
#endif
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }

      static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::remove(token, std::string_view(k));
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }
    };

    std::thread t(S::put_work, std::ref(kv1));
    S::put_work(std::ref(kv2));
    t.join();

    t = std::thread(S::remove_work, std::ref(token[0]), std::ref(kv1));
    S::remove_work(std::ref(token[1]), std::ref(kv2));
    t.join();

    ASSERT_EQ(masstree_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::leave(token[1]), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(multi_thread_delete_test, test9) {
  /**
   * Initial state : multi threads put until first split of interior.
   * Concurrent remove against initial state.
   */

  std::size_t ary_size = 241;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::string(1, i), std::to_string(i));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::string(1, i), std::to_string(i));
  }

#ifndef NDEBUG
  for (std::size_t h = 0; h < 5; ++h) {
#else
  for (std::size_t h = 0; h < 30; ++h) {
#endif
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);

    struct S {
      static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }

      static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::remove(token, std::string_view(k));
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }
    };

    std::thread t(S::put_work, std::ref(kv1));
    S::put_work(std::ref(kv2));
    t.join();

    t = std::thread(S::remove_work, std::ref(token[0]), std::ref(kv1));
    S::remove_work(std::ref(token[1]), std::ref(kv2));
    t.join();

    ASSERT_EQ(masstree_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::leave(token[1]), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(multi_thread_delete_test, test10) {
  /**
   * Initial state : multi threads put until first split of interior, which is using shuffled data.
   * Concurrent remove against initial state.
   */

  std::size_t ary_size = 241;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::string(1, i), std::to_string(i));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::string(1, i), std::to_string(i));
  }
  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 5; ++h) {
#else
  for (std::size_t h = 0; h < 30; ++h) {
#endif
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }

      static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::remove(token, std::string_view(k));
          if (ret != status::OK) {
            EXPECT_EQ(status::OK, ret); // output log
            std::abort();
          }
        }
      }
    };

    std::thread t(S::put_work, std::ref(kv1));
    S::put_work(std::ref(kv2));
    t.join();

    t = std::thread(S::remove_work, std::ref(token[0]), std::ref(kv1));
    S::remove_work(std::ref(token[1]), std::ref(kv2));
    t.join();

    ASSERT_EQ(masstree_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::leave(token[1]), status::OK);
    masstree_kvs::fin();
  }
}

}  // namespace yakushima::testing
