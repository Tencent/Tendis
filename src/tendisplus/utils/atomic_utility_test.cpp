// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "tendisplus/utils/atomic_utility.h"

namespace tendisplus {
TEST(Atom, Common) {
  Atom<uint64_t> v, v1;
  ++v;
  EXPECT_EQ(v._data.load(), uint64_t(1));
  v = v1;
  EXPECT_EQ(v._data.load(), uint64_t(0));
  ++v;
  ++v1;
  v += v1;
  EXPECT_EQ(v._data.load(), uint64_t(2));
}

}  // namespace tendisplus
