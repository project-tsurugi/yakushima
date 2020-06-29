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
  multi_thread_put_delete_test() {}

  ~multi_thread_put_delete_test() {}
};

TEST_F(multi_thread_put_delete_test, test1) {
  /**
   * concurrent put/delete same null char key slices and different key length to single border
   * by multi threads.
   */
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
  for (std::size_t h = 0; h < 1; ++h) {
#else
  for (std::size_t h = 0; h < 100; ++h) {
#endif
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);

    std::reverse(kv1.begin(), kv1.end());
    std::reverse(kv2.begin(), kv2.end());

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::remove(token, std::string_view(k));
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
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
}

TEST_F(multi_thread_put_delete_test, test2) {
  /**
   * test1 variant which is the test using shuffle order data.
   */

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
  for (std::size_t h = 0; h < 1; ++h) {
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
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::remove(token, std::string_view(k));
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
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
}

TEST_F(multi_thread_put_delete_test, test3) {
  /**
   * multiple put same null char key whose length is different each other against multiple border,
   * which is across some layer.
   */

  constexpr std::size_t ary_size = 15;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }

  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
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
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::remove(token, std::string_view(k));
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
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
}

TEST_F(multi_thread_put_delete_test, test4) {
  /**
   * test3 variant which is the test using shuffle order data.
   */

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

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
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
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::remove(token, std::string_view(k));
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
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
}

TEST_F(multi_thread_put_delete_test, test5) {
  /**
   * The number of puts that can be split only once and the deletes are repeated in multiple threads.
   */

  constexpr std::size_t ary_size = base_node::key_slice_length + 1;
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
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);

    std::reverse(kv1.begin(), kv1.end());
    std::reverse(kv2.begin(), kv2.end());

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::remove(token, std::string_view(k));
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
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
}

TEST_F(multi_thread_put_delete_test, test6) {
  /**
   * The number of puts that can be split only once and the deletes are repeated in multiple threads.
   * Use shuffled data.
   */

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

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
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
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::remove(token, std::string_view(k));
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
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
}

TEST_F(multi_thread_put_delete_test, test7) {
  /**
   * concurrent put/delete in the state between none to split of interior, which is using shuffled data.
   */

  constexpr std::size_t ary_size = interior_node::child_length * base_node::key_slice_length / 2;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    if (i <= UINT8_MAX) {
      kv1.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    } else {
      kv1.emplace_back(std::make_tuple(std::string(i / UINT8_MAX, UINT8_MAX) + std::string(1, i), std::to_string(i)));
    }
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    if (i <= UINT8_MAX) {
      kv2.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    } else {
      kv2.emplace_back(std::make_tuple(std::string(i / UINT8_MAX, UINT8_MAX) + std::string(1, i), std::to_string(i)));
    }
  }

  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
#else
  for (std::size_t h = 0; h < 200; ++h) {
#endif
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::remove(token, std::string_view(k));
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
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

    std::thread t(S::work, std::ref(kv2), std::ref(token[0]));
    S::work(std::ref(kv1), std::ref(token[1]));
    t.join();

    std::vector<std::tuple<char *, std::size_t>> tuple_list;
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k;
      if (i <= UINT8_MAX) {
        k = std::string(1, i);
      } else {
        k = std::string(i / UINT8_MAX, UINT8_MAX) + std::string(1, i);
      }
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
}

TEST_F(multi_thread_put_delete_test, test8) {
  /**
   * concurrent put/delete in the state between none to many split of interior.
   */

  constexpr std::size_t ary_size = interior_node::child_length * base_node::key_slice_length * 1.4;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    if (i <= UINT8_MAX) {
      kv1.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    } else {
      kv1.emplace_back(std::make_tuple(std::string(i / UINT8_MAX, UINT8_MAX) + std::string(1, i), std::to_string(i)));
    }
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    if (i <= UINT8_MAX) {
      kv2.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    } else {
      kv2.emplace_back(std::make_tuple(std::string(i / UINT8_MAX, UINT8_MAX) + std::string(1, i), std::to_string(i)));
    }
  }

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
#else
  for (std::size_t h = 0; h < 100; ++h) {
#endif
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);

    std::reverse(kv1.begin(), kv1.end());
    std::reverse(kv2.begin(), kv2.end());

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::remove(token, std::string_view(k));
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
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

    std::thread t(S::work, std::ref(kv2), std::ref(token[0]));
    S::work(std::ref(kv1), std::ref(token[1]));
    t.join();

    std::vector<std::tuple<char *, std::size_t>> tuple_list;
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k;
      if (i <= UINT8_MAX) {
        k = std::string(1, i);
      } else {
        k = std::string(i / UINT8_MAX, UINT8_MAX) + std::string(1, i);
      }
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
}

TEST_F(multi_thread_put_delete_test, test9) {
  /**
   * concurrent put/delete in the state between none to many split of interior with shuffle.
   */

  constexpr std::size_t ary_size = interior_node::child_length * base_node::key_slice_length * 1.4;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    if (i <= UINT8_MAX) {
      kv1.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    } else {
      kv1.emplace_back(
              std::make_tuple(std::string(i / UINT8_MAX, UINT8_MAX) + std::string(1, i), std::to_string(i)));
    }
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    if (i <= UINT8_MAX) {
      kv2.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    } else {
      kv2.emplace_back(
              std::make_tuple(std::string(i / UINT8_MAX, UINT8_MAX) + std::string(1, i), std::to_string(i)));
    }
  }

  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (size_t h = 0; h < 1; ++h) {
#else
  for (size_t h = 0; h < 1000; ++h) {
#endif
    masstree_kvs::init();
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::remove(token, std::string_view(k));
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
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

    std::thread t(S::work, std::ref(kv2), std::ref(token[0]));
    S::work(std::ref(kv1), std::ref(token[1]));
    t.join();

    std::vector<std::tuple<char *, std::size_t>> tuple_list;
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k;
      if (i <= UINT8_MAX) {
        k = std::string(1, i);
      } else {
        k = std::string(i / UINT8_MAX, UINT8_MAX) + std::string(1, i);
      }
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
}

TEST_F(multi_thread_put_delete_test, DISABLED_test10) {
  /**
   * multi-layer put-delete test.
   */

  constexpr std::size_t ary_size = interior_node::child_length * base_node::key_slice_length * 10;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    if (i <= UINT8_MAX) {
      kv1.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    } else {
      kv1.emplace_back(
              std::make_tuple(std::string(i / UINT8_MAX, UINT8_MAX) + std::string(1, i), std::to_string(i)));
    }
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    if (i <= UINT8_MAX) {
      kv2.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    } else {
      kv2.emplace_back(
              std::make_tuple(std::string(i / UINT8_MAX, UINT8_MAX) + std::string(1, i), std::to_string(i)));
    }
  }

  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (size_t h = 0; h < 100; ++h) {
#else
  for (size_t h = 0; h < 20; ++h) {
#endif
    masstree_kvs::init();
    ASSERT_EQ(base_node::get_root(), nullptr);
    Token token[2];
    ASSERT_EQ(masstree_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(masstree_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
            if (status::OK != ret) {
              ASSERT_EQ(status::OK, ret);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i)), v(std::get<1>(i));
            status ret = masstree_kvs::remove(token, std::string_view(k));
            if (status::OK != ret) {
              ASSERT_EQ(status::OK, ret);
              std::abort();
            }
          }
        }
        for (auto &i : kv) {
          std::string k(std::get<0>(i)), v(std::get<1>(i));
          status ret = masstree_kvs::put(std::string_view(k), v.data(), v.size());
          if (status::OK != ret) {
            ASSERT_EQ(status::OK, ret);
            std::abort();
          }
        }
      }
    };

    std::thread t(S::work, std::ref(kv2), std::ref(token[0]));
    S::work(std::ref(kv1), std::ref(token[1]));
    t.join();

    std::vector<std::tuple<char *, std::size_t>> tuple_list;
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size / 100; ++i) {
      std::string k;
      if (i <= UINT8_MAX) {
        k = std::string(1, i);
      } else {
        k = std::string(i / UINT8_MAX, UINT8_MAX) + std::string(1, i);
      }
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
}

}  // namespace yakushima::testing
