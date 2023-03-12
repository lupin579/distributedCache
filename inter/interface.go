package inter

import (
	"distributedCache/hash"
	"distributedCache/lru"
	"distributedCache/myHttp"
	"strings"
)

func Query(key string) string {
	hashValue, err := hash.GetHash(key)
	if err != nil {
		return err.Error()
	}
	slotPos := hashValue % uint32(64)
	result, err := myHttp.SelectCluster(key, slotPos)
	if err != nil {
		return err.Error()
	}
	return result
}

func CacheRange() (results string) {
	arr := make([]string, 0)
	for _, v := range lru.LruCache.GetHashMap() {
		arr = append(arr, v.Value)
	}
	results = strings.Join(arr, ":")
	return
}
