package main

import (
	"distributedCache/cluster"
	"distributedCache/conf"
	"distributedCache/inter"
	"distributedCache/lru"
	"distributedCache/monitor"
	"distributedCache/myHttp"
	"distributedCache/sentinel"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
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
	reqType := strings.Split(r.URL.String(), "/")[1]
	if reqType == "range" { //ip:port/range
		res := inter.CacheRange()
		w.Write([]byte(res))
		return
	}
	/*
		根据发送过来的请求报文将发送方的状态记录到本地的clusterNodes当中
		ps：req报文体中只会有发送方的ip，port，监管槽位
	*/
	if reqType == "meet" { //ip:port/meet
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
	if reqType == "redirect" { //ip:port/redirect
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
	if reqType == "status" { //读取当前节点信息
		bytes, err := json.Marshal(cluster.MyClusterNode)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		w.Header().Set("Content-Type", "application/json;charset=utf-8")
		w.Write(bytes)
		return
	}
	if reqType == "logoff" {
		defer r.Body.Close()
		status := new(sentinel.ReAssignStatus)
		var bytes []byte
		r.Body.Read(bytes)
		json.Unmarshal(bytes, status)
		tmpSlots := status.Status[fmt.Sprintf("%s:%d", cluster.MyClusterNode.Ip, cluster.MyClusterNode.Port)]
		for _, slot := range tmpSlots { //将临时负责的节点置一
			cluster.MyClusterNode.Slots[slot] = 1
		}
		ip, port := cluster.MyClusterNode.WhoHasThisSlot(uint32(tmpSlots[0])) //将之前监管此槽位的节点（即下线节点）拿出

		logoffNode := fmt.Sprintf("%s:%d", ip, port)
		delete(cluster.MyClusterNode.ClusterNodes, logoffNode)                                            //删除该节点
		delete(status.Status, fmt.Sprintf("%s:%d", cluster.MyClusterNode.Ip, cluster.MyClusterNode.Port)) //从分配任务中删除本节点
		for nd, slots := range status.Status {                                                            //遍历删除本地节点后的分配任务来更新本地记录的其他状态
			otherNode := cluster.MyClusterNode.ClusterNodes[nd]
			for _, slot := range slots {
				otherNode.Slots[slot] = 1
			}
			cluster.MyClusterNode.ClusterNodes[nd] = otherNode
		}
		w.Write([]byte("ok"))
	}
	if reqType == "recover" {
		sentinel.LogoffMu.Lock() //同步读出本节点状态是否为正在进行下线处理
		isHandling := sentinel.LogoffNodes[r.RemoteAddr]
		sentinel.LogoffMu.Unlock()
		for {
			if isHandling { //如果正在处理本节点那就等到
				continue
			} else { //处理结束再进行后续操作
				break
			}
		}
		oldSlots := sentinel.LogOffCache[r.RemoteAddr]
		tmp := sentinel.MySentinel.Slots
		for _, slot := range oldSlots { //将重上线节点的槽位还给他
			tmp[slot] = r.RemoteAddr
		}
		bytes, err := json.Marshal(tmp)
		if err != nil {
			w.Write([]byte(fmt.Sprintf("Marshal LogNodeCache failed:%s", err.Error())))
			return
		}
		w.Write(bytes)
		return
	}
	if reqType == "hb" { //普通节点响应心跳检测
		w.Write([]byte("alive"))
		return
	}
	http.Error(w, "nothing is here", 404)
}

func main() {
	conf.SetConfig()
	resp, err := http.Get(fmt.Sprintf("http://%s:%d/recover", conf.YamlConfig.MainNodeIP, conf.YamlConfig.MainNodePort))
	if err != nil {
		log.Fatalln("get logOff from sentinel failed")
	}
	bytes, err := sentinel.RespReader(resp) //bytes是返回的本节点下线前监控的槽位
	if err != nil {
		log.Fatalln("read resp failed")
	}
	slots := make([]int, 0)
	if strings.Contains(string(bytes), "[") {
		err := json.Unmarshal(bytes, slots)
		if err != nil {
			log.Fatalln("Unmarshal Sentinel's Resp, oldSlots failed")
		}
	} else {
		log.Fatalln("sentinel has some problems")
	}
	if conf.YamlConfig.Identity == "clusterNode" {
		lru.Constructor(500)
		cluster.ClusterNodeInit(slots)
		go monitor.Sysmon()
		if err := myHttp.SetPeerStatus(); err != nil {
			panic("MeetPeers failed")
		}
	} else {
		if err := sentinel.SentinelInit(); err != nil {
			panic("failed to init sentinel")
		}
	}
	var h htp = 10
	http.ListenAndServe("0.0.0.0:80", &h)
}
