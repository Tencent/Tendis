package main

import (
	"integrate_test/util"
	"math/rand"
)

func SpecifHashData(m *util.RedisServer, auth string, prefixKey string) {
	util.InsertAndExpiredHashData(m, auth, 1, 1000, 1, prefixKey+"_hash1")
	util.InsertAndExpiredHashData(m, auth, 2, 1000, 120*1000, prefixKey+"_hash2")
	util.InsertAndExpiredHashData(m, auth, 3, 1000, int(rand.Int31n(1000)), prefixKey+"_hash3")

	util.InsertAndExpiredHashData(m, auth, 4, 999, 1, prefixKey+"_hash4")
	util.InsertAndExpiredHashData(m, auth, 5, 999, 120*1000, prefixKey+"_hash5")
	util.InsertAndExpiredHashData(m, auth, 6, 999, int(rand.Int31n(1000)), prefixKey+"_hash6")

	util.InsertAndExpiredHashData(m, auth, 7, 1001, 1, prefixKey+"_hash7")
	util.InsertAndExpiredHashData(m, auth, 8, 1001, 60*1000*1000, prefixKey+"_hash8")
	util.InsertAndExpiredHashData(m, auth, 9, 1001, int(rand.Int31n(1000)), prefixKey+"_hash9")

	util.InsertAndExpiredHashData(m, auth, 10, 3000, 1, prefixKey+"_hash10")
	util.InsertAndExpiredHashData(m, auth, 11, 3000, 120*1000, prefixKey+"_hash11")
	util.InsertAndExpiredHashData(m, auth, 12, 3000, 400000, prefixKey+"_hash12")
}
