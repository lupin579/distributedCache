package main

import (
	"distributedCache/cluster"
	"distributedCache/conf"
	"distributedCache/inter"
	"distributedCache/lru"
	"distributedCache/monitor"
	"distributedCache/myHttp"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
)

type htp int

func (h *htp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.String() == "/" { //ip:port/ 用于查询kv，格式：key:keyName
		bytes, err := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			http.Error(w, "server busy", http.StatusInternalServerError)
			return
		}
		body := string(bytes)
		key := strings.Split(body, ":")[1]
		result := inter.Query(key)
		w.Write([]byte(result))
		return
	}
	if strings.Split(r.URL.String(), "/")[1] == "range" { //ip:port/range
		res := inter.CacheRange()
		w.Write([]byte(res))
		return
	}
	if strings.Split(r.URL.String(), "/")[1] == "meet" { //ip:port/meet
		bytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		body := string(bytes)
		status := strings.Split(body, ":")
		fmt.Println(status)
		port, err := strconv.Atoi(status[1])
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		err = cluster.MyClusterNode.AppendPeer(status[0], port, status[2], status[3])
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		w.Write([]byte(fmt.Sprintf("%s:%d:%d:%d", conf.YamlConfig.IP,
			conf.YamlConfig.Port,
			conf.YamlConfig.ClusterStart,
			conf.YamlConfig.ClusterEnd)))
		return
	}
	if strings.Split(r.URL.String(), "/")[1] == "redirect" { //ip:port/redirect
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "server busy", http.StatusInternalServerError)
			return
		}
		resp := lru.LruCache.Get(string(body))
		if resp != "-1" { //当前节点中存在本kv，将value返回
			w.Write([]byte(fmt.Sprintf("redis:%s", resp)))
			return
		} else { //不存在，从mysql中查出并放入
			if cluster.MyBlackList.Query(string(body)) { //先查看本地黑名单有无该key，有则刷新存在时间并返回不存在
				w.Write([]byte("that value is not in mysql too"))
				return
			}
			result := strconv.Itoa(rand.Int()) //模拟从mysql查出
			if len(result) == 3 {              //模拟mysql中也没有
				cluster.MyBlackList.Append(string(body)) //加入黑名单
				w.Write([]byte("that value is not in mysql too"))
				return
			} else { //模拟mysql中有该数据
				node := lru.LruCache.Put(string(body), result, 30) //放入当前lru队列中
				cluster.MyClusterNode.SetKV(string(body), node)    //放入当前集群节点kv中
				w.Write([]byte(fmt.Sprintf("mysql:%s", result)))
				return
			}
		}
		return
	}
	if strings.Split(r.URL.String(), "/")[1] == "status" { //读取当前节点信息
		bytes, err := json.Marshal(cluster.MyClusterNode)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		w.Header().Set("Content-Type", "application/json;charset=utf-8")
		w.Write(bytes)
		return
	}
	http.Error(w, "nothing is here", 404)
}

func main() {
	conf.SetConfig()
	lru.Constructor(500)
	cluster.ClusterNodeInit()
	go monitor.Sysmon()
	if err := myHttp.SetPeerStatus(); err != nil {
		panic("MeetPeers failed")
	}
	var h htp = 10
	http.ListenAndServe("0.0.0.0:80", &h)
}
