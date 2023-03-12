package lru

import (
	"distributedCache/model"
	"sync"
	"time"
)

var head model.Node
var tail model.Node
var LruCache LRUCache

func setNode() model.Node {
	return model.Node{
		Key:       "0",
		Value:     "0",
		Prev:      nil,
		Next:      nil,
		ExpiresAt: 0,
	}
}

type LRUCache struct {
	mu      sync.Mutex
	len     int
	cap     int
	hashMap map[string]*model.Node
}

func (this *LRUCache) GetCap() int {
	return this.cap
}

func (this *LRUCache) GetHashMap() map[string]*model.Node {
	return this.hashMap
}

func Constructor(capacity int) {
	head = model.Node{
		Specific: true,
		Key:      "0",
		Value:    "0",
		Prev:     nil,
		Next:     nil,
	}
	tail = model.Node{
		Specific: true,
		Key:      "0",
		Value:    "-100",
		Prev:     nil,
		Next:     nil,
	}
	LruCache = LRUCache{
		len:     0,
		cap:     capacity,
		hashMap: make(map[string]*model.Node, capacity),
	}
}

func (this *LRUCache) Get(key string) string {
	this.mu.Lock()
	defer this.mu.Unlock()
	if value, isExist := this.hashMap[key]; isExist {
		tmp := this.hashMap[key]
		count := 3
		if count >= this.cap { //count>cap时遍历查看次数设置为容量大小（即cap还不到热点区之外时就不会因访问而改变位置）
			count = this.cap
		}
		if !listPos(this.hashMap[key], count) { //当前节点不在前三个中才换位
			tmp.Prev.Next = tmp.Next
			tmp.Next.Prev = tmp.Prev
			tmp.Next = head.Next
			tmp.Prev = &head
			head.Next = tmp
			tmp.Next.Prev = tmp
		}
		return value.Value
	} else {
		return "-1"
	}

}

/*
expire 有效时间（秒）
*/
func (this *LRUCache) Put(key string, value string, expire int64) *model.Node {
	this.mu.Lock()
	defer this.mu.Unlock()
	if _, IsExist := this.hashMap[key]; !IsExist { //新加入的节点
		if this.len+1 > this.cap { //超出容量
			delKey := tail.Prev.Key
			tail.Prev.Prev.Next = &tail
			tail.Prev = tail.Prev.Prev
			delete(this.hashMap, delKey)
			this.len--
		}
		this.hashMap[key] = &model.Node{
			Specific:  false,
			Key:       key,
			Value:     value,
			Prev:      nil,
			Next:      nil,
			ExpiresAt: time.Now().Add(30 * time.Second).Unix(), //当前时间+有效时间
		}
		this.len++
		if head.Next == nil { //表中没有节点时添加节点
			head.Next = this.hashMap[key]
			this.hashMap[key].Prev = &head
			tail.Prev = this.hashMap[key]
			this.hashMap[key].Next = &tail
			return this.hashMap[key]
		}
		this.hashMap[key].Next = head.Next
		head.Next = this.hashMap[key]
		this.hashMap[key].Next.Prev = this.hashMap[key]
		this.hashMap[key].Prev = &head
		return this.hashMap[key]
	} else { //之前有过的节点
		this.hashMap[key].Value = value
		tmp := this.hashMap[key]
		count := 3
		if count > this.cap {
			count = this.cap
		}
		if !listPos(tmp, count) { //不在前三个中才换位
			tmp.Prev.Next = tmp.Next
			tmp.Next.Prev = tmp.Prev
			tmp.Next = head.Next
			tmp.Prev = &head
			head.Next = tmp
			tmp.Next.Prev = tmp
		}
		return nil
	}
}
