#pragma once

#include "scheme.h"

#include "gtest/gtest.h"

#define ASSERT_OK(expr) ASSERT_EQ(expr, yakushima::status::OK)
