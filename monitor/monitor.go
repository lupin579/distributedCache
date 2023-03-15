package monitor

import (
	"distributedCache/cluster"
	"distributedCache/lru"
	"time"
)

/*
Sysmon
检测过期node并删除
执行周期2s
*/
func Sysmon() {
	ticker := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-ticker.C:
			if lru.LruCache.GetCap() > 5 {
				count := 0
				for k, v := range lru.LruCache.GetHashMap() {
					if v.IsExpired() {
						lru.LruCache.DelNode(k)
					}
					count++
					if count >= 5 { //随机扫描五个后退出本次扫描
						break
					}
				}
			}
			cluster.MyClusterNode.CleanDeletedNode()
		}
	}
}
