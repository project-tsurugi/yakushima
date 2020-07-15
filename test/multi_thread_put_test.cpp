/**
 * @file multi_thread_put_test.cpp
 */

#include <algorithm>
#include <random>
#include <thread>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

class mtpt : public ::testing::Test {
protected:
  mtpt() {}

  ~mtpt() {}
};

TEST_F(mtpt, test1) { // NOLINT
  constexpr std::size_t ary_size = 9;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < 5; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  for (std::size_t i = 5; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }

#ifndef NDEBUG
  for (std::size_t h = 0; h < 20; ++h) {
#else
  for (std::size_t h = 0; h < 100; ++h) {
#endif
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);

    std::reverse(kv1.begin(), kv1.end());
    std::reverse(kv2.begin(), kv2.end());

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            ASSERT_EQ(ret, status::OK);
            std::abort();
          }
        }
      }
    };

    std::thread t(S::work, std::ref(kv2));
    S::work(std::ref(kv1));
    t.join();

    std::vector<std::tuple<char *, std::size_t>> tuple_list;
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k(i, '\0');
      masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false,
                               tuple_list);
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(masstree_kvs::leave(token), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(mtpt, test2) { // NOLINT

  constexpr std::size_t ary_size = 9;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < 5; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  for (std::size_t i = 5; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 5; ++h) {
#else
  for (std::size_t h = 0; h < 100; ++h) {
#endif
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            ASSERT_EQ(status::OK, ret);
            std::abort();
          }
        }
      }
    };

    std::thread t(S::work, std::ref(kv2));
    S::work(std::ref(kv1));
    t.join();

    std::vector<std::tuple<char *, std::size_t>> tuple_list;
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k(i, '\0');
      masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false,
                               tuple_list);
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(masstree_kvs::leave(token), status::OK);
    masstree_kvs::destroy();
    masstree_kvs::fin();
  }
}

TEST_F(mtpt, test3) { // NOLINT

  constexpr std::size_t ary_size = 15;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < 7; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  for (std::size_t i = 7; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }

#ifndef NDEBUG
  for (std::size_t h = 0; h < 20; ++h) {
#else
  for (std::size_t h = 0; h < 100; ++h) {
#endif
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);

    std::reverse(kv1.begin(), kv1.end());
    std::reverse(kv2.begin(), kv2.end());

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            ASSERT_EQ(ret, status::OK);
            std::abort();
          }
        }
      }
    };

    std::thread t(S::work, std::ref(kv2));
    S::work(std::ref(kv1));
    t.join();

    std::vector<std::tuple<char *, std::size_t>> tuple_list;
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k(i, '\0');
      masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false,
                               tuple_list);
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(masstree_kvs::leave(token), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(mtpt, test4) { // NOLINT

  constexpr std::size_t ary_size = 15;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < 7; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  for (std::size_t i = 7; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 5; ++h) {
#else
  for (std::size_t h = 0; h < 30; ++h) {
#endif
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            ASSERT_EQ(ret, status::OK);
            std::abort();
          }
        }
      }
    };

    std::thread t(S::work, std::ref(kv2));
    S::work(std::ref(kv1));
    t.join();

    std::vector<std::tuple<char *, std::size_t>> tuple_list;
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k(i, '\0');
      masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false,
                               tuple_list);
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(masstree_kvs::leave(token), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(mtpt, test5) { // NOLINT

  constexpr std::size_t ary_size = base_node::key_slice_length + 1;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(1, 'a' + i), std::to_string(i)));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(1, 'a' + i), std::to_string(i)));
  }

#ifndef NDEBUG
  for (std::size_t h = 0; h < 3; ++h) {
#else
  for (std::size_t h = 0; h < 100; ++h) {
#endif
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);

    std::reverse(kv1.begin(), kv1.end());
    std::reverse(kv2.begin(), kv2.end());

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            ASSERT_EQ(ret, status::OK);
            std::abort();
          }
        }
      }
    };

    std::thread t(S::work, std::ref(kv2));
    S::work(std::ref(kv1));
    t.join();

    std::vector<std::tuple<char *, std::size_t>> tuple_list;
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k(1, 'a' + i);
      masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false,
                               tuple_list);
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(masstree_kvs::leave(token), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(mtpt, test6) { // NOLINT

  constexpr std::size_t ary_size = base_node::key_slice_length + 1;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(1, 'a' + i), std::to_string(i)));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(1, 'a' + i), std::to_string(i)));
  }
  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
#else
  for (std::size_t h = 0; h < 100; ++h) {
#endif
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            ASSERT_EQ(ret, status::OK);
            std::abort();
          }
        }
      }
    };

    std::thread t(S::work, std::ref(kv2));
    S::work(std::ref(kv1));
    t.join();

    std::vector<std::tuple<char *, std::size_t>> tuple_list;
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k(1, 'a' + i);
      masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false,
                               tuple_list);
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(masstree_kvs::leave(token), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(mtpt, test7) { // NOLINT

  constexpr std::size_t ary_size = 100;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
  }

#ifndef NDEBUG
  for (size_t h = 0; h < 1; ++h) {
#else
  for (size_t h = 0; h < 100; ++h) {
#endif
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);

    std::reverse(kv1.begin(), kv1.end());
    std::reverse(kv2.begin(), kv2.end());

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            ASSERT_EQ(ret, status::OK);
            std::abort();
          }
        }
      }
    };

    std::thread t(S::work, std::ref(kv2));
    S::work(std::ref(kv1));
    t.join();

    std::vector<std::tuple<char *, std::size_t>> tuple_list;
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k(1, i);
      masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false,
                               tuple_list);
      if (tuple_list.size() != i + 1) {
        EXPECT_EQ(tuple_list.size(), i + 1);
      }
      ASSERT_EQ(tuple_list.size(), i + 1);
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(masstree_kvs::leave(token), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(mtpt, test8) { // NOLINT

  constexpr std::size_t ary_size = 100;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
  }

  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
#else
  for (std::size_t h = 0; h < 100; ++h) {
#endif
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            ASSERT_EQ(ret, status::OK);
            std::abort();
          }
        }
      }
    };

    std::thread t(S::work, std::ref(kv2));
    S::work(std::ref(kv1));
    t.join();

    std::vector<std::tuple<char *, std::size_t>> tuple_list;
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k(1, i);
      masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false,
                               tuple_list);
      if (tuple_list.size() != i + 1) {
        EXPECT_EQ(tuple_list.size(), i + 1);
      }
      ASSERT_EQ(tuple_list.size(), i + 1);
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(masstree_kvs::leave(token), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(mtpt, test9) { // NOLINT

  std::size_t ary_size = 241;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
  }

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
#else
  for (std::size_t h = 0; h < 100; ++h) {
#endif
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);

    std::reverse(kv1.begin(), kv1.end());
    std::reverse(kv2.begin(), kv2.end());

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            ASSERT_EQ(ret, status::OK);
            std::abort();
          }
        }
      }
    };

    std::thread t(S::work, std::ref(kv2));
    S::work(std::ref(kv1));
    t.join();

    std::vector<std::tuple<char *, std::size_t>> tuple_list;
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k(1, i);
      masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false,
                               tuple_list);
      ASSERT_EQ(tuple_list.size(), i + 1);
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(masstree_kvs::leave(token), status::OK);
    masstree_kvs::fin();
  }
}

TEST_F(mtpt, test10) { // NOLINT

  std::size_t ary_size = 241;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
  }

  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
#else
  for (std::size_t h = 0; h < 100; ++h) {
#endif
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (ret != status::OK) {
            ASSERT_EQ(ret, status::OK);
            std::abort();
          }
        }
      }
    };

    std::thread t(S::work, std::ref(kv2));
    S::work(std::ref(kv1));
    t.join();

    std::vector<std::tuple<char *, std::size_t>> tuple_list;
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k(1, i);
      masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false, tuple_list);
      if (tuple_list.size() != i + 1) {
        ASSERT_EQ(tuple_list.size(), i + 1);
      }

      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(masstree_kvs::leave(token), status::OK);

    masstree_kvs::fin();
  }
}

}  // namespace yakushima::testing
