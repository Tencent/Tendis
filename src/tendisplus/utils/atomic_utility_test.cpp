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
