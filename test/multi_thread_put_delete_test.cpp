/**
 * @file multi_thread_put_delete_test.cpp
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

class multi_thread_put_delete_test : public ::testing::Test {
protected:
  multi_thread_put_delete_test() {
    masstree_kvs::init();
  }

  ~multi_thread_put_delete_test() {
    masstree_kvs::fin();
  }
};

TEST_F(multi_thread_put_delete_test, test1) {
  /**
   * concurrent put/delete same null char key slices and different key length to single border
   * by multi threads.
   */
  Token token[2];
  ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
  ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);
  constexpr std::size_t ary_size = 9;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < 5; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  for (std::size_t i = 5; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }

  struct S {
    static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
      for (std::size_t j = 0; j < 10; ++j) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
        }
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          ASSERT_EQ(status::OK, masstree_kvs::remove(token, std::string_view(k)));
        }
      }
      for (auto &i : kv) {
        std::string k(std::get<0>(i)), v(std::get<1>(i));
        ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
      }
    }
  };

  std::thread t(S::work, std::ref(kv2), std::ref(token[0]));
  S::work(std::ref(kv1), std::ref(token[1]));
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
  ASSERT_EQ(masstree_kvs::leave(token[0]), status::OK);
  ASSERT_EQ(masstree_kvs::leave(token[1]), status::OK);
}

#if 0
TEST_F(multi_thread_put_delete_test,
       put_same_null_char_key_slices_and_different_key_length_to_single_border_by_multi_thread_with_shuffle) {
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
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
  std::shuffle(kv1.begin(), kv1.end(), engine);
  std::shuffle(kv2.begin(), kv2.end(), engine);

  struct S {
    static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
      for (auto &i : kv) {
        std::string k(std::get<0>(i)), v(std::get<1>(i));
        ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
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
}

TEST_F(multi_thread_put_delete_test,
       put_same_null_char_key_slices_and_different_key_length_to_multiple_border_by_multi_thread) {
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  constexpr std::size_t ary_size = 15;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < 7; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  for (std::size_t i = 7; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }

  struct S {
    static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
      for (auto &i : kv) {
        std::string k(std::get<0>(i)), v(std::get<1>(i));
        ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
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
}

TEST_F(multi_thread_put_delete_test,
       put_same_null_char_key_slices_and_different_key_length_to_multiple_border_by_multi_thread_with_shuffle) {
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
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
  std::shuffle(kv1.begin(), kv1.end(), engine);
  std::shuffle(kv2.begin(), kv2.end(), engine);

  struct S {
    static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
      for (auto &i : kv) {
        std::string k(std::get<0>(i)), v(std::get<1>(i));
        ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
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
}

TEST_F(multi_thread_put_delete_test, put_until_creating_interior_node) {
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  constexpr std::size_t ary_size = base_node::key_slice_length + 1;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(1, 'a' + i), std::to_string(i)));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(1, 'a' + i), std::to_string(i)));
  }

  struct S {
    static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
      for (auto &i : kv) {
        std::string k(std::get<0>(i)), v(std::get<1>(i));
        ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
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
}

TEST_F(multi_thread_put_delete_test, put_until_creating_interior_node_with_shuffle) {
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
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
  std::shuffle(kv1.begin(), kv1.end(), engine);
  std::shuffle(kv2.begin(), kv2.end(), engine);

  struct S {
    static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
      for (auto &i : kv) {
        std::string k(std::get<0>(i)), v(std::get<1>(i));
        ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
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
}

TEST_F(multi_thread_put_delete_test, put_between_split_border_and_split_interior_with_no_shuffle) {
  masstree_kvs::fin(); // fit to test constructor.
  for (size_t i = 0; i < 100; ++i) {
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);
    constexpr std::size_t ary_size = 100;
    std::vector<std::tuple<std::string, std::string>> kv1;
    std::vector<std::tuple<std::string, std::string>> kv2;
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
      kv1.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
      kv2.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    }

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
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
  masstree_kvs::init(); // fit to test destructor.
}

TEST_F(multi_thread_put_delete_test, put_between_split_border_and_split_interior_with_shuffle) {
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
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
  std::shuffle(kv1.begin(), kv1.end(), engine);
  std::shuffle(kv2.begin(), kv2.end(), engine);

  struct S {
    static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
      for (auto &i : kv) {
        std::string k(std::get<0>(i)), v(std::get<1>(i));
        ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
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
}

TEST_F(multi_thread_put_delete_test, put_until_first_split_of_interior_node_with_no_shuffle) {
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  std::size_t ary_size = 241;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
  }

  struct S {
    static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
      for (auto &i : kv) {
        std::string k(std::get<0>(i)), v(std::get<1>(i));
        ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
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
}

TEST_F(multi_thread_put_delete_test, put_until_first_split_of_interior_node_with_shuffle) {
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
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
  std::shuffle(kv1.begin(), kv1.end(), engine);
  std::shuffle(kv2.begin(), kv2.end(), engine);

  struct S {
    static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
      for (auto &i : kv) {
        std::string k(std::get<0>(i)), v(std::get<1>(i));
        ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
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
      ASSERT_EQ(tuple_list.size(), i + 1);
    }
    for (std::size_t j = 0; j < i + 1; ++j) {
      std::string v(std::to_string(j));
      ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
    }
  }
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

#endif

}  // namespace yakushima::testing
