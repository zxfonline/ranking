package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/zxfonline/ranking"
)

var (
	maxUID int64 = 10
)

func init() {
	rand.Seed(time.Now().Local().UnixNano())
}

func main() {
	rt := ranking.GetRankTree(1)
	for uid := int64(1); uid <= maxUID; uid++ {
		time.Sleep(time.Nanosecond)
		rt.AddRankInfo(uid, uid, time.Now().UTC().UnixNano())
	}

	err := ranking.SaveRanking(rt, "./rank1.txt")
	if err != nil {
		panic(err)
	}

	fmt.Println("====QueryByRank=====")
	for rank := int32(1); rank <= int32(maxUID); rank++ {
		info := rt.QueryByRank(rank)
		fmt.Printf("%+v\n", info)
	}
	fmt.Println("====QueryRankInfo=====")
	for uid := int64(1); uid <= maxUID; uid++ {
		info := rt.QueryRankInfo(uid)
		fmt.Printf("%+v\n", info)
	}

	rt, err = ranking.LoadRanking("./rank1.txt")
	if err != nil {
		panic(err)
	}
	fmt.Println("=====================")
	ranking.ResetRankTree(1, rt)
	rt = ranking.GetRankTree(1)
	fmt.Println("====QueryByRank=====")
	for rank := int32(1); rank <= int32(maxUID); rank++ {
		info := rt.QueryByRank(rank)
		fmt.Printf("%+v\n", info)
	}
	fmt.Println("====QueryRankInfo=====")
	for uid := int64(1); uid <= maxUID; uid++ {
		info := rt.QueryRankInfo(uid)
		fmt.Printf("%+v\n", info)
	}
}
