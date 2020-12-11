//
// Created by takenliu on 2020/12/10.
//

#ifndef TENDIS_PLUS_RAND_H
#define TENDIS_PLUS_RAND_H

#define NN	16
#define MASK	((1 << (NN - 1)) + (1 << (NN - 1)) - 1)
#define LOW(x)	((unsigned)(x) & MASK)
#define HIGH(x)	LOW((x) >> NN)
#define MUL(x, y, z)	{ int32_t l = (long)(x) * (long)(y); \
		(z)[0] = LOW(l); (z)[1] = HIGH(l); }
#define CARRY(x, y)	((int32_t)(x) + (long)(y) > MASK)
#define ADDEQU(x, y, z)	(z = CARRY(x, (y)), x = LOW(x + (y)))
#define X0	0x330E
#define X1	0xABCD
#define X2	0x1234
#define A0	0xE66D
#define A1	0xDEEC
#define A2	0x5
#define C	0xB
#define SET3(x, x0, x1, x2)	((x)[0] = (x0), (x)[1] = (x1), (x)[2] = (x2))
#define SETLOW(x, y, n) SET3(x, LOW((y)[n]), LOW((y)[(n)+1]), LOW((y)[(n)+2]))
#define SEED(x0, x1, x2) (SET3(x, x0, x1, x2), SET3(a, A0, A1, A2), c = C)
#define REST(v)	for (i = 0; i < 3; i++) { xsubi[i] = x[i]; x[i] = temp[i]; } \
		return (v);
#define HI_BIT	(1L << (2 * NN - 1))

class RedisRandom {
public:
  int32_t redisLrand48();

  void redisSrand48(int32_t seedval);

private:
  void next(void);

private:
  uint32_t x[3] = { X0, X1, X2 }, a[3] = { A0, A1, A2 }, c = C;
};
#define REDIS_LRAND48_MAX INT32_MAX

#endif //TENDIS_PLUS_RAND_H
