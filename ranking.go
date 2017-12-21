// Copyright 2016 zxfonline@sina.com. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ranking

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"os"
	"sync"
	"time"
)

const (
	// 跳跃表最大层数
	SKIPLIST_MAXLEVEL = 32
	// 随机概率
	SKIPLIST_P = 0.25
)

var (
	_RTS  map[int16]*RankTree
	_Lock sync.RWMutex
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type RankInfo struct {
	Id        int64
	Val       int64
	Rank      int32
	Timestamp int64
}

func less(a, b int64) bool {
	return a > b
}

func (info1 *RankInfo) cmp(info2 *RankInfo) int {
	if info1.Id == info2.Id {
		return 0
	}
	if info1.Timestamp < info2.Timestamp {
		return -1
	} else if info1.Timestamp > info2.Timestamp {
		return 1
	}
	if info1.Id < info2.Id {
		return -1
	} else if info1.Id > info2.Id {
		return 1
	}
	return 0
}

type skiplistlevel struct {
	Forward *skiplistnode
	Span    int32
}

type skiplistnode struct {
	Key   int64
	Val   *RankInfo
	Level []skiplistlevel
}

type skiplist struct {
	Header *skiplistnode
	Tail   *skiplistnode
	Length int32
	Level  int32
}

func newSkiplistNode(level int32, key int64, val *RankInfo) *skiplistnode {
	node := new(skiplistnode)
	node.Key = key
	node.Val = val
	node.Level = make([]skiplistlevel, level)
	for i := level - 1; i >= 0; i-- {
		node.Level[i].Forward = nil
	}
	return node
}

func newSkiplist() *skiplist {
	sl := new(skiplist)
	sl.Header = newSkiplistNode(SKIPLIST_MAXLEVEL, -1, nil)
	for i := 0; i < SKIPLIST_MAXLEVEL; i++ {
		sl.Header.Level[i].Forward = nil
		sl.Header.Level[i].Span = 0
	}
	sl.Tail = nil
	sl.Level = 1
	return sl
}

func randomLevel() int32 {
	lvl := int32(1)
	for rand.Float32() < SKIPLIST_P && lvl < SKIPLIST_MAXLEVEL {
		lvl++
	}
	return lvl
}

func (sl *skiplist) insert(key int64, val *RankInfo) {
	var update [SKIPLIST_MAXLEVEL]*skiplistnode
	var rank [SKIPLIST_MAXLEVEL]int32
	x := sl.Header
	for i := sl.Level - 1; i >= 0; i-- {
		if i == sl.Level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.Level[i].Forward != nil &&
			(less(x.Level[i].Forward.Key, key) ||
				(x.Level[i].Forward.Key == key &&
					x.Level[i].Forward.Val.cmp(val) < 0)) {
			rank[i] += x.Level[i].Span
			x = x.Level[i].Forward
		}
		update[i] = x
	}

	level := randomLevel()
	if level > sl.Level {
		for i := sl.Level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.Header
			update[i].Level[i].Span = sl.Length
		}
		sl.Level = level
	}

	x = newSkiplistNode(level, key, val)
	for i := int32(0); i < level; i++ {
		x.Level[i].Forward = update[i].Level[i].Forward
		update[i].Level[i].Forward = x
		n := rank[0] - rank[i]
		x.Level[i].Span = update[i].Level[i].Span - n
		update[i].Level[i].Span = rank[0] - rank[i] + 1
	}

	for i := level; i < sl.Level; i++ {
		update[i].Level[i].Span++
	}
	sl.Length++
}

func (sl *skiplist) remove(key int64, val *RankInfo) bool {
	var update [SKIPLIST_MAXLEVEL]*skiplistnode
	x := sl.Header
	for i := sl.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil &&
			(less(x.Level[i].Forward.Key, key) ||
				(x.Level[i].Forward.Key == key &&
					x.Level[i].Forward.Val.cmp(val) < 0)) {
			x = x.Level[i].Forward
		}
		update[i] = x
	}
	x = x.Level[0].Forward
	if x != nil && x.Key == key && x.Val.cmp(val) == 0 {
		// delete node
		for i := int32(0); i < sl.Level; i++ {
			if update[i].Level[i].Forward == x {
				update[i].Level[i].Span += x.Level[i].Span - 1
				update[i].Level[i].Forward = x.Level[i].Forward
			} else {
				update[i].Level[i].Span--
			}
		}

		for sl.Level > 1 && sl.Header.Level[sl.Level-1].Forward == nil {
			sl.Level--
		}
		sl.Length--
		return true
	}
	return false
}

func (sl *skiplist) search(key int64, val *RankInfo) bool {
	x := sl.Header
	for i := sl.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil &&
			(less(x.Level[i].Forward.Key, key) ||
				(x.Level[i].Forward.Key == key &&
					x.Level[i].Forward.Val.cmp(val) < 0)) {
			//x.Level[i].Forward.Val.(int) < val.(int))) {
			x = x.Level[i].Forward
		}
	}
	x = x.Level[0].Forward
	return x != nil && x.Key == key && x.Val.cmp(val) == 0
}

func (sl *skiplist) rank(key int64, val *RankInfo) int32 {
	rank := int32(0)
	x := sl.Header
	for i := sl.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil &&
			(less(x.Level[i].Forward.Key, key) ||
				(x.Level[i].Forward.Key == key &&
					x.Level[i].Forward.Val.cmp(val) < 0)) {
			//x.Level[i].Forward.Val.(int) < val.(int))) {
			rank += x.Level[i].Span
			x = x.Level[i].Forward
		}
	}
	x = x.Level[0].Forward
	if x != nil && x.Key == key && x.Val.cmp(val) == 0 {
		return rank
	}
	return -1
}

func (sl *skiplist) searchByRank(rank int32) (int64, *RankInfo) {
	visited := int32(0)
	x := sl.Header
	for i := sl.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil && (visited+x.Level[i].Span) <= rank {
			visited += x.Level[i].Span
			x = x.Level[i].Forward
		}
		if visited == rank {
			return x.Key, x.Val
		}
	}
	return -1, nil
}

func (sl *skiplist) getFirstByRank(rank int32) *skiplistnode {
	visited := int32(0)
	x := sl.Header
	for i := sl.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil && (visited+x.Level[i].Span) <= rank {
			visited += x.Level[i].Span
			x = x.Level[i].Forward
		}
		if visited == rank {
			return x
		}
	}
	return nil
}

func copyValue(v *RankInfo) *RankInfo {
	val := &RankInfo{
		Id:        v.Id,
		Val:       v.Val,
		Rank:      v.Rank,
		Timestamp: v.Timestamp,
	}
	return val
}

func (sl *skiplist) searchByRankRange(min, max int32) []*RankInfo {
	res := make([]*RankInfo, 0)
	st := sl.getFirstByRank(min)
	if st == nil {
		return nil
	}

	rank := min
	for i := st; rank <= max && i != nil; i = i.Level[0].Forward {
		i.Val.Rank = rank
		val := copyValue(i.Val)
		res = append(res, val)
		rank++
	}
	return res
}

func (sl *skiplist) foreach(do func(int64, interface{})) {
	x := sl.Header
	for i := x.Level[0].Forward; i != nil; i = i.Level[0].Forward {
		do(i.Key, i.Val)
	}
}

type RankTree struct {
	Sl           *skiplist
	EntryMapping map[int64]*RankInfo
	lock         sync.RWMutex
}

func NewRankTree() *RankTree {
	rt := new(RankTree)
	rt.Sl = newSkiplist()
	rt.EntryMapping = make(map[int64]*RankInfo)
	return rt
}

// 删除排名信息
func (rt *RankTree) RemoveRankInfo(uid int64) bool {
	rt.lock.Lock()
	defer rt.lock.Unlock()
	if info := rt.EntryMapping[uid]; info != nil {
		rt.Sl.remove(info.Val, info)
		delete(rt.EntryMapping, uid)
		return true
	}
	return false
}

// 添加新排名信息
func (rt *RankTree) AddRankInfo(uid int64, val int64, timestamp int64) {
	rt.lock.Lock()
	defer rt.lock.Unlock()
	if info := rt.EntryMapping[uid]; info == nil {
		info = new(RankInfo)
		info.Id = uid

		info.Val = val
		info.Timestamp = timestamp
		rt.Sl.insert(info.Val, info)

		rt.EntryMapping[uid] = info
	} else if info.Val != val {
		rt.Sl.remove(info.Val, info)

		info.Val = val
		info.Timestamp = timestamp
		rt.Sl.insert(info.Val, info)
	}
}

// 更新排名信息
func (rt *RankTree) UpdateRankInfo(uid int64, val int64, timestamp int64) {
	rt.lock.Lock()
	defer rt.lock.Unlock()
	if info := rt.EntryMapping[uid]; info == nil {
		info = new(RankInfo)
		info.Id = uid

		info.Val = val
		info.Timestamp = timestamp
		rt.Sl.insert(info.Val, info)

		rt.EntryMapping[uid] = info
	} else if info.Val != val {
		rt.Sl.remove(info.Val, info)

		info.Val = val
		info.Timestamp = timestamp
		rt.Sl.insert(info.Val, info)
	}
}

// 更新排名信息
func (rt *RankTree) IncrRankInfo(uid int64, val int64, timestamp int64) {
	rt.lock.Lock()
	defer rt.lock.Unlock()
	if info := rt.EntryMapping[uid]; info == nil {
		info = new(RankInfo)
		info.Id = uid

		info.Val = val
		info.Timestamp = timestamp
		rt.Sl.insert(info.Val, info)

		rt.EntryMapping[uid] = info
	} else {
		rt.Sl.remove(info.Val, info)

		info.Val += val
		info.Timestamp = timestamp
		rt.Sl.insert(info.Val, info)
	}
}

// 查询用户排名
func (rt *RankTree) QueryRankInfo(uid int64) *RankInfo {
	rt.lock.RLock()
	defer rt.lock.RUnlock()
	var info *RankInfo
	if info = rt.EntryMapping[uid]; info == nil {
		return nil
	}
	info.Rank = rt.Sl.rank(info.Val, info) + 1
	return info
}

// 查询指定范围排名
func (rt *RankTree) QueryByRankRange(min, max int32) []*RankInfo {
	if min > max {
		return nil
	}
	if min <= 0 {
		min = 1
	}
	if max > rt.Sl.Length {
		max = rt.Sl.Length
	}
	return rt.Sl.searchByRankRange(min, max)
}

// 根据排名查询信息
func (rt *RankTree) QueryByRank(rank int32) *RankInfo {
	key, val := rt.Sl.searchByRank(rank)
	if key < 0 {
		return nil
	} else if val != nil {
		val.Rank = rank
	}
	return val
}

// 从dump加载排名模块
func LoadRanking(filename string) (*RankTree, error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, _ := f.Stat()
	raw := make([]byte, info.Size())
	_, err = f.Read(raw)
	if err != nil {
		return nil, err
	}
	rt := new(RankTree)
	enc := gob.NewDecoder(bytes.NewReader(raw))
	err = enc.Decode(rt)
	if err != nil {
		return nil, err
	}
	return rt, nil
}

// dump排名模块
func SaveRanking(rt *RankTree, filename string) (bool, error) {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return false, err
	}
	defer f.Close()
	buffer := new(bytes.Buffer)
	enc := gob.NewEncoder(buffer)
	err = enc.Encode(rt)
	if err != nil {
		return false, err
	}
	f.Write(buffer.Bytes())
	return true, nil
}

type DbRankInfo struct {
	Type      int16
	Id        int64
	Val       int64
	Timestamp int64
}

func Load(infos []DbRankInfo) {
	LoadRankTrees(infos)
}

// 从dump加载排名模块
func LoadRankTrees(infos []DbRankInfo) map[int16]*RankTree {
	// construct ranktrees
	rts := make(map[int16]*RankTree)
	var rt *RankTree
	for _, info := range infos {
		rt = rts[info.Type]
		if rt == nil {
			rt = NewRankTree()
			rts[info.Type] = rt
		}
		rt.UpdateRankInfo(info.Id, info.Val, info.Timestamp)
	}
	return rts
}

func Save() []DbRankInfo {
	_Lock.RLock()
	defer _Lock.RUnlock()
	return SaveRankTrees(_RTS)
}

// dump排名模块
func SaveRankTrees(rts map[int16]*RankTree) []DbRankInfo {
	infos := make([]DbRankInfo, 0)
	for t, rt := range rts {
		for _, entry := range rt.EntryMapping {
			info := DbRankInfo{
				Type:      t,
				Id:        entry.Id,
				Val:       entry.Val,
				Timestamp: entry.Timestamp,
			}
			infos = append(infos, info)
		}
	}
	return infos
}

func GetRankTree(rtype int16) *RankTree {
	_Lock.Lock()
	defer _Lock.Unlock()
	if rt, ok := _RTS[rtype]; !ok {
		rt = NewRankTree()
		_RTS[rtype] = rt
		return rt
	} else {
		return rt
	}
}

func ResetRankTree(rtype int16) {
	_Lock.Lock()
	defer _Lock.Unlock()
	delete(_RTS, rtype)
	_RTS[rtype] = NewRankTree()
}
