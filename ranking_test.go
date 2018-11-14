package ranking

import (
	"math/rand"
	"testing"
	"time"
)

func randInt(min int64, max int64) int64 {
	return min + rand.Int63n(max-min)
}

var (
	maxVal int64
	maxUID int64
)

func init() {
	rand.Seed(time.Now().Local().UnixNano())
	maxVal = 200000
	maxUID = 100000
}

func TestAddQueryRankInfo(t *testing.T) {
	rt := NewRankTree()
	for uid := int64(1); uid <= maxUID; uid++ {
		rt.AddRankInfo(uid, maxUID-uid+1, time.Now().UTC().UnixNano())
	}
	for uid := int64(1); uid <= maxUID; uid++ {
		info := rt.QueryRankInfo(uid)
		if uid != int64(info.Rank) {
			t.Logf("TestAddQueryRank: uid=%d, rank=%d", uid, info.Rank)
			t.Fail()
		}
	}
}

func TestUpdateRankInfo(t *testing.T) {
	rt := NewRankTree()
	for uid := int64(1); uid <= maxUID; uid++ {
		val := randInt(int64(0), maxVal-int64(1))
		rt.AddRankInfo(uid, val, time.Now().UTC().UnixNano())
	}
	for uid := int64(1); uid <= maxUID; uid++ {
		newVal := uid
		rt.AddRankInfo(uid, maxUID-newVal+1, time.Now().UTC().UnixNano())
	}
	for uid := int64(1); uid <= maxUID; uid++ {
		info := rt.QueryRankInfo(int64(uid))
		if uid != int64(info.Rank) {
			t.Logf("TestUpdateRankInfo: uid=%d, rank=%d, val=%d", uid, info.Rank, info.Val)
			t.Fail()
		}
	}
}

func BenchmarkAddRankInfo(b *testing.B) {
	rt := NewRankTree()
	for i := 0; i < b.N; i++ {
		uid := randInt(1, maxUID)
		val := randInt(0, maxVal)
		rt.AddRankInfo(int64(uid), val, time.Now().UTC().UnixNano())
	}
}

func BenchmarkQueryRankInfo(b *testing.B) {
	rt := NewRankTree()
	for i := 0; i < b.N; i++ {
		uid := randInt(1, maxUID)
		rt.QueryRankInfo(int64(uid))
	}
}

func BenchmarkUpdateRankInfo(b *testing.B) {
	rt := NewRankTree()
	var newval int64
	for i := 0; i < b.N; i++ {
		uid := randInt(1, maxUID)
		newval = randInt(0, maxVal)
		rt.AddRankInfo(int64(uid), newval, time.Now().UTC().Unix())
	}
}
