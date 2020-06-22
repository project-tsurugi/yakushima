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
  masstree_kvs::fin();
  for (std::size_t h = 0; h < 30; ++h) {
    masstree_kvs::init();
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
    masstree_kvs::fin();
  }
  masstree_kvs::init();
}

TEST_F(multi_thread_put_delete_test, test2) {
  /**
   * test1 variant which is the test using shuffle order data.
   */
  masstree_kvs::fin();
  for (std::size_t h = 0; h < 30; ++h) {
    masstree_kvs::init();
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

    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());
    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);
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
    masstree_kvs::fin();
  }
  masstree_kvs::init();
}

TEST_F(multi_thread_put_delete_test, test3) {
  /**
   * multiple put same null char key whose length is different each other against multiple border,
   * which is across some layer.
   */
  masstree_kvs::fin();
  for (std::size_t h = 0; h < 20; ++h) {
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);
    constexpr std::size_t ary_size = 15;
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
    masstree_kvs::fin();
  }
  masstree_kvs::init();
}

TEST_F(multi_thread_put_delete_test, test4) {
  /**
   * test3 variant which is the test using shuffle order data.
   */
  masstree_kvs::fin();
  for (std::size_t h = 0; h < 30; ++h) {
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);
    constexpr std::size_t ary_size = 15;
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
    masstree_kvs::fin();
  }
  masstree_kvs::init();
}

TEST_F(multi_thread_put_delete_test, test5) {
  /**
   * The number of puts that can be split only once and the deletes are repeated in multiple threads.
   */
  masstree_kvs::fin();
  for (std::size_t h = 0; h < 50; ++h) {
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);
    constexpr std::size_t ary_size = base_node::key_slice_length + 1;
    std::vector<std::tuple<std::string, std::string>> kv1;
    std::vector<std::tuple<std::string, std::string>> kv2;
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
      kv1.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
      kv2.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
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
    for (std::size_t i = 1; i < ary_size; ++i) {
      std::string k(1, i);
      masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false,
                               tuple_list);
      if (tuple_list.size() != i + 1) {
        masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false,
                                 tuple_list);
        ASSERT_EQ(tuple_list.size(), i + 1);
      }
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(masstree_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::leave(token[1]), status::OK);
    masstree_kvs::fin();
  }
  masstree_kvs::init();
}

TEST_F(multi_thread_put_delete_test, test6) {
  /**
   * The number of puts that can be split only once and the deletes are repeated in multiple threads.
   * Use shuffled data.
   */
  masstree_kvs::fin();
  for (std::size_t h = 0; h < 30; ++h) {
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);
    constexpr std::size_t ary_size = base_node::key_slice_length + 1;
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
    for (std::size_t i = 1; i < ary_size; ++i) {
      std::string k(1, i);
      masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false,
                               tuple_list);
      if (tuple_list.size() != i + 1) {
        masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false,
                                 tuple_list);
        ASSERT_EQ(tuple_list.size(), i + 1);
      }
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(masstree_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::leave(token[1]), status::OK);
    masstree_kvs::fin();
  }
  masstree_kvs::init();
}

TEST_F(multi_thread_put_delete_test, DISABLED_test7) {
  /**
   * put_between_split_border_and_split_interior_with_no_shuffle
   */
  masstree_kvs::fin(); // fit to test constructor.
  for (size_t h = 0; h < 10; ++h) {
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);
    constexpr std::size_t ary_size = interior_node::child_length * base_node::key_slice_length * 3 / 2;
    /**
     * *3 / 2 means that if 1.5 times the number of keys that make the interior node full, then the interior node is
     * sometimes full, sometimes not.
     */
    std::vector<std::tuple<std::string, std::string>> kv1;
    std::vector<std::tuple<std::string, std::string>> kv2;
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
      kv1.emplace_back(std::make_tuple(std::to_string(i), std::to_string(i)));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
      kv2.emplace_back(std::make_tuple(std::to_string(i), std::to_string(i)));
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
      masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(std::to_string(i)), false,
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
    ASSERT_EQ(masstree_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::leave(token[1]), status::OK);
    masstree_kvs::fin();
  }
  masstree_kvs::init(); // fit to test destructor.
}

#if 0
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
