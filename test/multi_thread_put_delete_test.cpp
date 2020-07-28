/**
 * @file multi_thread_put_delete_test.cpp
 */

#include <algorithm>
#include <random>
#include <thread>
#include <tuple>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

class mtpdt : public ::testing::Test {
};

TEST_F(mtpdt, test1) { // NOLINT
  /**
   * concurrent put/delete same null char key slices and different key length to single border
   * by multi threads.
   */
  constexpr std::size_t ary_size = 9;
  std::vector<std::tuple<std::string, std::string>> kv1; // NOLINT
  std::vector<std::tuple<std::string, std::string>> kv2; // NOLINT
  for (std::size_t i = 0; i < 5; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  for (std::size_t i = 5; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 500; ++h) {
#endif
    yakushima_kvs::init();
    std::array<Token, 2> token{};
    ASSERT_EQ(yakushima_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::enter(token[1]), status::OK);

    std::reverse(kv1.begin(), kv1.end());
    std::reverse(kv2.begin(), kv2.end());

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            char **dummy{};
            status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            status ret = yakushima_kvs::remove(token, k);
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
        for (auto &i : kv) {
          std::string k(std::get<0>(i));
          std::string v(std::get<1>(i));
          char **dummy{};
          status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
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

    std::vector<std::tuple<char *, std::size_t>> tuple_list; // NOLINT
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k(i, '\0');
      yakushima_kvs::scan<char>("", false, k, false, tuple_list);
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(yakushima_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::leave(token[1]), status::OK);
    yakushima_kvs::fin();
  }
}

TEST_F(mtpdt, test2) { // NOLINT
  /**
   * test1 variant which is the test using shuffle order data.
   */

  constexpr std::size_t ary_size = 9;
  std::vector<std::tuple<std::string, std::string>> kv1; // NOLINT
  std::vector<std::tuple<std::string, std::string>> kv2; // NOLINT
  for (std::size_t i = 0; i < 5; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  for (std::size_t i = 5; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }

  std::random_device seed_gen{};
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 500; ++h) {
#endif
    yakushima_kvs::init();
    std::array<Token, 2> token{};
    ASSERT_EQ(yakushima_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            char **dummy{};
            status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            status ret = yakushima_kvs::remove(token, k);
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
        for (auto &i : kv) {
          std::string k(std::get<0>(i));
          std::string v(std::get<1>(i));
          char **dummy{};
          status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
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

    std::vector<std::tuple<char *, std::size_t>> tuple_list; // NOLINT
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k(i, '\0');
      yakushima_kvs::scan<char>("", false, k, false, tuple_list);
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(yakushima_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::leave(token[1]), status::OK);
    yakushima_kvs::fin();
  }
}

TEST_F(mtpdt, test3) { // NOLINT
  /**
   * multiple put same null char key whose length is different each other against multiple border,
   * which is across some layer.
   */

  constexpr std::size_t ary_size = 15;
  std::vector<std::tuple<std::string, std::string>> kv1; // NOLINT
  std::vector<std::tuple<std::string, std::string>> kv2; // NOLINT
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }

  std::random_device seed_gen{};
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 100; ++h) {
#endif
    yakushima_kvs::init();
    std::array<Token, 2> token{};
    ASSERT_EQ(yakushima_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);
    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            char **dummy{};
            status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            status ret = yakushima_kvs::remove(token, k);
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
        for (auto &i : kv) {
          std::string k(std::get<0>(i));
          std::string v(std::get<1>(i));
          char **dummy{};
          status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
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

    std::vector<std::tuple<char *, std::size_t>> tuple_list; // NOLINT
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k(i, '\0');
      yakushima_kvs::scan<char>("", false, k, false, tuple_list);
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(yakushima_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::leave(token[1]), status::OK);
    yakushima_kvs::fin();
  }
}

TEST_F(mtpdt, test4) { // NOLINT
  /**
   * test3 variant which is the test using shuffle order data.
   */

  constexpr std::size_t ary_size = 15;
  std::vector<std::tuple<std::string, std::string>> kv1; // NOLINT
  std::vector<std::tuple<std::string, std::string>> kv2; // NOLINT
  for (std::size_t i = 0; i < 5; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  for (std::size_t i = 5; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }

  std::random_device seed_gen{};
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 100; ++h) {
#endif
    yakushima_kvs::init();
    std::array<Token, 2> token{};
    ASSERT_EQ(yakushima_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            char **dummy{};
            status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            status ret = yakushima_kvs::remove(token, k);
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
        for (auto &i : kv) {
          std::string k(std::get<0>(i));
          std::string v(std::get<1>(i));
          char **dummy{};
          status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
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

    std::vector<std::tuple<char *, std::size_t>> tuple_list; // NOLINT
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k(i, '\0');
      yakushima_kvs::scan<char>("", false, k, false, tuple_list);
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(yakushima_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::leave(token[1]), status::OK);
    yakushima_kvs::fin();
  }
}

TEST_F(mtpdt, test5) { // NOLINT
  /**
   * The number of puts that can be split only once and the deletes are repeated in multiple threads.
   */

  constexpr std::size_t ary_size = base_node::key_slice_length + 1;
  std::vector<std::tuple<std::string, std::string>> kv1; // NOLINT
  std::vector<std::tuple<std::string, std::string>> kv2; // NOLINT
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
    yakushima_kvs::init();
    std::array<Token, 2> token{};
    ASSERT_EQ(yakushima_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::enter(token[1]), status::OK);

    std::reverse(kv1.begin(), kv1.end());
    std::reverse(kv2.begin(), kv2.end());

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            char **dummy{};
            status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            status ret = yakushima_kvs::remove(token, k);
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
        for (auto &i : kv) {
          std::string k(std::get<0>(i));
          std::string v(std::get<1>(i));
          char **dummy{};
          status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
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

    std::vector<std::tuple<char *, std::size_t>> tuple_list; // NOLINT
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 1; i < ary_size; ++i) {
      std::string k(1, i);
      yakushima_kvs::scan<char>("", false, k, false, tuple_list);
      if (tuple_list.size() != i + 1) {
        yakushima_kvs::scan<char>("", false, k, false, tuple_list);
        ASSERT_EQ(tuple_list.size(), i + 1);
      }
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(yakushima_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::leave(token[1]), status::OK);
    yakushima_kvs::fin();
  }
}

TEST_F(mtpdt, test5_2) { // NOLINT
  /**
   * The number of puts that can be split only once and the deletes are repeated in multiple threads.
   * 5_2 : this situations in multiple layer.
   */

  constexpr std::size_t ary_size = base_node::key_slice_length + 1;
  std::vector<std::tuple<std::string, std::string>> kv1; // NOLINT
  std::vector<std::tuple<std::string, std::string>> kv2; // NOLINT
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(8, INT8_MAX) + std::string(1, i), std::to_string(i)));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(8, INT8_MAX) + std::string(1, i), std::to_string(i)));
  }

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 100; ++h) {
#endif
    yakushima_kvs::init();
    std::array<Token, 2> token{};
    ASSERT_EQ(yakushima_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::enter(token[1]), status::OK);

    std::reverse(kv1.begin(), kv1.end());
    std::reverse(kv2.begin(), kv2.end());

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            char **dummy{};
            status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            status ret = yakushima_kvs::remove(token, k);
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
        for (auto &i : kv) {
          std::string k(std::get<0>(i));
          std::string v(std::get<1>(i));
          char **dummy{};
          status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
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

    std::vector<std::tuple<char *, std::size_t>> tuple_list; // NOLINT
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 1; i < ary_size; ++i) {
      std::string k = std::string(8, INT8_MAX) + std::string(1, i);
      yakushima_kvs::scan<char>("", false, k, false, tuple_list);
      if (tuple_list.size() != i + 1) {
        yakushima_kvs::scan<char>("", false, k, false, tuple_list);
        if (tuple_list.size() != i + 1) {
          yakushima_kvs::scan<char>("", false, k, false, tuple_list);
          ASSERT_EQ(tuple_list.size(), i + 1);
        }
      }
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(yakushima_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::leave(token[1]), status::OK);
    yakushima_kvs::fin();
  }
}

TEST_F(mtpdt, test6) { // NOLINT
  /**
   * The number of puts that can be split only once and the deletes are repeated in multiple threads.
   * Use shuffled data.
   */

  constexpr std::size_t ary_size = base_node::key_slice_length + 1;
  std::vector<std::tuple<std::string, std::string>> kv1; // NOLINT
  std::vector<std::tuple<std::string, std::string>> kv2; // NOLINT
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
    yakushima_kvs::init();
    std::array<Token, 2> token{};
    ASSERT_EQ(yakushima_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            char **dummy{};
            status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            status ret = yakushima_kvs::remove(token, k);
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
        for (auto &i : kv) {
          std::string k(std::get<0>(i));
          std::string v(std::get<1>(i));
          char **dummy{};
          status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
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

    std::vector<std::tuple<char *, std::size_t>> tuple_list; // NOLINT
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 1; i < ary_size; ++i) {
      std::string k(1, i);
      yakushima_kvs::scan<char>("", false, k, false, tuple_list);
      if (tuple_list.size() != i + 1) {
        yakushima_kvs::scan<char>("", false, k, false, tuple_list);
        ASSERT_EQ(tuple_list.size(), i + 1);
      }
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(yakushima_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::leave(token[1]), status::OK);
    yakushima_kvs::fin();
  }
}

TEST_F(mtpdt, test6_2) { // NOLINT
  /**
   * multiple layer version of test6
   */

  constexpr std::size_t ary_size = base_node::key_slice_length + 1;
  std::vector<std::tuple<std::string, std::string>> kv1; // NOLINT
  std::vector<std::tuple<std::string, std::string>> kv2; // NOLINT
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(8, INT8_MAX) + std::string(1, i), std::to_string(i)));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(8, INT8_MAX) + std::string(1, i), std::to_string(i)));
  }

  std::random_device seed_gen{};
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 100; ++h) {
#endif
    yakushima_kvs::init();
    std::array<Token, 2> token{};
    ASSERT_EQ(yakushima_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            char **dummy{};
            status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            status ret = yakushima_kvs::remove(token, k);
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
        for (auto &i : kv) {
          std::string k(std::get<0>(i));
          std::string v(std::get<1>(i));
          char **dummy{};
          status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
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

    std::vector<std::tuple<char *, std::size_t>> tuple_list; // NOLINT
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 1; i < ary_size; ++i) {
      std::string k = std::string(8, INT8_MAX) + std::string(1, i);
      yakushima_kvs::scan<char>("", false, k, false, tuple_list);
      if (tuple_list.size() != i + 1) {
        yakushima_kvs::scan<char>("", false, k, false, tuple_list);
        ASSERT_EQ(tuple_list.size(), i + 1);
      }
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(yakushima_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::leave(token[1]), status::OK);
    yakushima_kvs::fin();
  }
}

TEST_F(mtpdt, test7) { // NOLINT
  /**
   * concurrent put/delete in the state between none to split of interior, which is using shuffled data.
   */

  constexpr std::size_t ary_size = interior_node::child_length * base_node::key_slice_length / 2;
  std::vector<std::tuple<std::string, std::string>> kv1; // NOLINT
  std::vector<std::tuple<std::string, std::string>> kv2; // NOLINT
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
  }

  std::random_device seed_gen{};
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 200; ++h) {
#endif
    yakushima_kvs::init();
    std::array<Token, 2> token{};
    ASSERT_EQ(yakushima_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            char **dummy{};
            status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            status ret = yakushima_kvs::remove(token, k);
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
        for (auto &i : kv) {
          std::string k(std::get<0>(i));
          std::string v(std::get<1>(i));
          char **dummy{};
          status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
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

    std::vector<std::tuple<char *, std::size_t>> tuple_list; // NOLINT
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k;
      k = std::string(1, i);
      yakushima_kvs::scan<char>("", false, k, false, tuple_list);
      if (tuple_list.size() != i + 1) {
        yakushima_kvs::scan<char>("", false, k, false, tuple_list);
        ASSERT_EQ(tuple_list.size(), i + 1);
      }
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(yakushima_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::leave(token[1]), status::OK);
    yakushima_kvs::fin();
  }
}

TEST_F(mtpdt, test7_2) { // NOLINT
  /**
   * multiple layer version of test 7
   */

  constexpr std::size_t ary_size = interior_node::child_length * base_node::key_slice_length / 2;
  std::vector<std::tuple<std::string, std::string>> kv1; // NOLINT
  std::vector<std::tuple<std::string, std::string>> kv2; // NOLINT
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(8, INT8_MAX) + std::string(1, i), std::to_string(i)));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(8, INT8_MAX) + std::string(1, i), std::to_string(i)));
  }

  std::random_device seed_gen{};
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 200; ++h) {
#endif
    yakushima_kvs::init();
    std::array<Token, 2> token{};
    ASSERT_EQ(yakushima_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            char **dummy{};
            status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            status ret = yakushima_kvs::remove(token, k);
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
        for (auto &i : kv) {
          std::string k(std::get<0>(i));
          std::string v(std::get<1>(i));
          char **dummy{};
          status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
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

    std::vector<std::tuple<char *, std::size_t>> tuple_list; // NOLINT
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k;
      k = std::string(8, INT8_MAX) + std::string(1, i);
      yakushima_kvs::scan<char>("", false, k, false, tuple_list);
      if (tuple_list.size() != i + 1) {
        yakushima_kvs::scan<char>("", false, k, false, tuple_list);
        ASSERT_EQ(tuple_list.size(), i + 1);
      }
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(yakushima_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::leave(token[1]), status::OK);
    yakushima_kvs::fin();
  }
}

TEST_F(mtpdt, test8) { // NOLINT
  /**
   * concurrent put/delete in the state between none to many split of interior.
   */

  constexpr std::size_t ary_size = interior_node::child_length * base_node::key_slice_length * 1.4;
  std::vector<std::tuple<std::string, std::string>> kv1; // NOLINT
  std::vector<std::tuple<std::string, std::string>> kv2; // NOLINT
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    if (i <= INT8_MAX) {
      kv1.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    } else {
      kv1.emplace_back(
              std::make_tuple(std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i - INT8_MAX), std::to_string(i)));
    }
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    if (i <= INT8_MAX) {
      kv2.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    } else {
      kv2.emplace_back(
              std::make_tuple(std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i - INT8_MAX), std::to_string(i)));
    }
  }

#ifndef NDEBUG
  for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 100; ++h) {
#endif
    yakushima_kvs::init();
    std::array<Token, 2> token{};
    ASSERT_EQ(yakushima_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::enter(token[1]), status::OK);

    std::reverse(kv1.begin(), kv1.end());
    std::reverse(kv2.begin(), kv2.end());

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            char **dummy{};
            status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            status ret = yakushima_kvs::remove(token, k);
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
        for (auto &i : kv) {
          std::string k(std::get<0>(i));
          std::string v(std::get<1>(i));
          char **dummy{};
          status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
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

    std::vector<std::tuple<char *, std::size_t>> tuple_list; // NOLINT
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k;
      if (i <= INT8_MAX) {
        k = std::string(1, i);
      } else {
        k = std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i - INT8_MAX);
      }
      yakushima_kvs::scan<char>("", false, k, false, tuple_list);
      if (tuple_list.size() != i + 1) {
        yakushima_kvs::scan<char>("", false, k, false, tuple_list);
        ASSERT_EQ(tuple_list.size(), i + 1);
      }
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(yakushima_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::leave(token[1]), status::OK);
    yakushima_kvs::fin();
  }
}

TEST_F(mtpdt, test9) { // NOLINT
  /**
   * concurrent put/delete in the state between none to many split of interior with shuffle.
   */

  constexpr std::size_t ary_size = interior_node::child_length * base_node::key_slice_length * 1.4;
  std::vector<std::tuple<std::string, std::string>> kv1; // NOLINT
  std::vector<std::tuple<std::string, std::string>> kv2; // NOLINT
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    if (i <= INT8_MAX) {
      kv1.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    } else {
      kv1.emplace_back(
              std::make_tuple(std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i - INT8_MAX), std::to_string(i)));
    }
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    if (i <= INT8_MAX) {
      kv2.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    } else {
      kv2.emplace_back(
              std::make_tuple(std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i - INT8_MAX), std::to_string(i)));
    }
  }

  std::random_device seed_gen{};
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (size_t h = 0; h < 1; ++h) {
#else
    for (size_t h = 0; h < 100; ++h) {
#endif
    yakushima_kvs::init();
    std::array<Token, 2> token{};
    ASSERT_EQ(yakushima_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            char **dummy{};
            status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string k(std::get<0>(i));
            std::string v(std::get<1>(i));
            status ret = yakushima_kvs::remove(token, k);
            if (ret != status::OK) {
              ASSERT_EQ(ret, status::OK);
              std::abort();
            }
          }
        }
        for (auto &i : kv) {
          std::string k(std::get<0>(i));
          std::string v(std::get<1>(i));
          char **dummy{};
          status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
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

    std::vector<std::tuple<char *, std::size_t>> tuple_list; // NOLINT
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      std::string k;
      if (i <= INT8_MAX) {
        k = std::string(1, i);
      } else {
        k = std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i - INT8_MAX);
      }
      yakushima_kvs::scan<char>("", false, k, false, tuple_list);
      if (tuple_list.size() != i + 1) {
        yakushima_kvs::scan<char>("", false, k, false, tuple_list);
        ASSERT_EQ(tuple_list.size(), i + 1);
      }
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(yakushima_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::leave(token[1]), status::OK);
    yakushima_kvs::fin();
  }
}

TEST_F(mtpdt, test10) { // NOLINT
  /**
   * multi-layer put-delete test.
   */

  constexpr std::size_t ary_size = interior_node::child_length * base_node::key_slice_length * 10;
  std::vector<std::tuple<std::string, std::string>> kv1; // NOLINT
  std::vector<std::tuple<std::string, std::string>> kv2; // NOLINT
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    if (i <= INT8_MAX) {
      kv1.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    } else {
      kv1.emplace_back(
              std::make_tuple(std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i - INT8_MAX), std::to_string(i)));
    }
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    if (i <= INT8_MAX) {
      kv2.emplace_back(std::make_tuple(std::string(1, i), std::to_string(i)));
    } else {
      kv2.emplace_back(
              std::make_tuple(std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i - INT8_MAX), std::to_string(i)));
    }
  }

  std::random_device seed_gen{};
  std::mt19937 engine(seed_gen());

#ifndef NDEBUG
  for (size_t h = 0; h < 1; ++h) {
#else
    for (size_t h = 0; h < 20; ++h) {
#endif
    yakushima_kvs::init();
    ASSERT_EQ(base_node::get_root(), nullptr);
    std::array<Token, 2> token{};
    ASSERT_EQ(yakushima_kvs::enter(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::enter(token[1]), status::OK);

    std::shuffle(kv1.begin(), kv1.end(), engine);
    std::shuffle(kv2.begin(), kv2.end(), engine);

    struct S {
      static void work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
        for (std::size_t j = 0; j < 10; ++j) {
          for (auto &i : kv) {
            std::string v(std::get<1>(i));
            std::string k(std::get<0>(i));
            char **dummy{};
            status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
            if (status::OK != ret) {
              ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
              ASSERT_EQ(status::OK, ret);
              std::abort();
            }
          }
          for (auto &i : kv) {
            std::string v(std::get<1>(i));
            std::string k(std::get<0>(i));
            status ret = yakushima_kvs::remove(token, k);
            if (status::OK != ret) {
              ret = yakushima_kvs::remove(token, k);
              ASSERT_EQ(status::OK, ret);
              std::abort();
            }
          }
        }
        for (auto &i : kv) {
          std::string k(std::get<0>(i));
          std::string v(std::get<1>(i));
          char **dummy{};
          status ret = yakushima_kvs::put(k, v.data(), dummy, v.size());
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

    std::vector<std::tuple<char *, std::size_t>> tuple_list; // NOLINT
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size / 100; ++i) {
      std::string k;
      if (i <= INT8_MAX) {
        k = std::string(1, i);
      } else {
        k = std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i - INT8_MAX);
      }
      yakushima_kvs::scan<char>("", false, k, false, tuple_list);
      if (tuple_list.size() != i + 1) {
        yakushima_kvs::scan<char>("", false, k, false, tuple_list);
        ASSERT_EQ(tuple_list.size(), i + 1);
      }
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(yakushima_kvs::leave(token[0]), status::OK);
    ASSERT_EQ(yakushima_kvs::leave(token[1]), status::OK);
    yakushima_kvs::fin();
  }
}

}  // namespace yakushima::testing
