package lru

import (
	"distributedCache/model"
)

func nodePos(nd *model.Node, pos int) bool {
	for i := 0; i < pos-1; i++ {
		if nd.Prev.Specific == false {
			nd = nd.Prev
		} else {
			if i+1 >= pos-1 {
				return true
			} else {
				return false
			}
		}
	}
	if nd.Prev.Specific == false {
		return false
	}
	return true
}

func listPos(nd *model.Node, pos int) bool {
	for i := 0; i < pos; i++ {
		if nodePos(nd, pos) {
			return true
		}
	}
	return false
}

func (cache *LRUCache) DelNode(key string) {
	nd := cache.hashMap[key]
	nd.Prev.Next = nd.Next
	nd.Next.Prev = nd.Prev
	delete(cache.hashMap, key)
}
