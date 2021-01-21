// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <stdint.h>
#include "tendisplus/script/rand.h"

int32_t RedisRandom::redisLrand48() {
  next();
  return (((int32_t)x[2] << (NN - 1)) + (x[1] >> 1));
}

void RedisRandom::redisSrand48(int32_t seedval) {
  SEED(X0, LOW(seedval), HIGH(seedval));
}

void RedisRandom::next(void) {
  uint32_t p[2], q[2], r[2], carry0, carry1;

  MUL(a[0], x[0], p);
  ADDEQU(p[0], c, carry0);
  ADDEQU(p[1], carry0, carry1);
  MUL(a[0], x[1], q);
  ADDEQU(p[1], q[0], carry0);
  MUL(a[1], x[0], r);
  x[2] = LOW(carry0 + carry1 + CARRY(p[1], r[0]) + q[1] + r[1] +
             a[0] * x[2] + a[1] * x[1] + a[2] * x[0]);
  x[1] = LOW(p[1] + r[0]);
  x[0] = LOW(p[0]);
}
