// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_UTILS_ATOMIC_UTILITY_H_
#define SRC_TENDISPLUS_UTILS_ATOMIC_UTILITY_H_

#include <atomic>
#include <memory>
#include <iostream>
#include "gtest/gtest.h"

namespace tendisplus {

template <typename T>
class Atom {
 public:
  Atom() : _data(0) {}

  Atom(const T& v) : _data(v) {}  // NOLINT

  Atom(Atom&& v) : _data(v._data.load(RLX)) {
    v._data.store(0, RLX);
  }

  Atom(const Atom& v) : _data(v._data.load(RLX)) {}

  Atom operator-(const Atom& right) {
    Atom result;
    auto v = _data.load(RLX);
    auto vv = right._data.load(RLX);
    result._data.store(v - vv, RLX);
    return result;
  }

  Atom& operator=(const Atom& other) {
    _data.store(other._data.load(RLX), RLX);
    return *this;
  }

  Atom& operator+=(const Atom& other) {
    _data.store(_data.load(RLX) + other._data.load(RLX), RLX);
    return *this;
  }

  Atom& operator++() {
    ++_data;
    return *this;
  }

  Atom& operator--() {
    --_data;
    return *this;
  }

  T get() const {
    return _data.load(RLX);
  }

  friend std::ostream& operator<<(std::ostream& os, const Atom& v) {
    os << v._data;
    return os;
  }

 private:
  FRIEND_TEST(Atom, Common);
  std::atomic<T> _data;
  static constexpr auto RLX = std::memory_order_relaxed;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_UTILS_ATOMIC_UTILITY_H_
