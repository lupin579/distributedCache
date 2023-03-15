package sentinel

import (
	"distributedCache/cluster"
	"distributedCache/conf"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var MySentinel Sentinel

//下线名单缓存
var LogoffMu sync.Mutex
var LogoffNodes map[string]bool = make(map[string]bool, 0) //bool代表是否正在进行该节点的下线工作，防止有节点刚下线就上线导致并发问题
var LogOffCache map[string][]int = make(map[string][]int, 0)

/*
Sentinel
kv节点的存储的缓存时间是否需要设置过期时间是否需要
*/
type Sentinel struct {
	mu          sync.Mutex
	nodeRetTime map[string]int64 //各节点的上次响应的时间
	Slots       [64]string       //各槽位监管状况
}

type ReAssignStatus struct {
	Status map[string][]int
}

/*
SentinelInit
哨兵节点的初始化工作
初始化MySentinel结构体
*/
func SentinelInit() error {
	resp, err := http.Get(fmt.Sprintf("http://%s:%d/status", conf.YamlConfig.MainNodeIP, conf.YamlConfig.MainNodePort))
	if err != nil {
		log.Printf("failed to get mainNode status:%s", err.Error())
		return err
	}
	content, err := RespReader(resp)
	if err != nil {
		log.Printf("failed to get resp body:%s", err.Error())
		return err
	}
	status := cluster.ClusterNode{
		Slots:        [64]uint8{},
		Kv:           nil,
		ClusterNodes: nil,
		Ip:           "",
		Port:         0,
	}
	json.Unmarshal(content, &status)
	MySentinel.SetSentinelContent(status) //填入各节点的信息
	go MySentinel.HeartBeatCheck()
	return nil
}

func (sentinel *Sentinel) HeartBeatCheck() {
	ticker1 := time.NewTicker(2 * time.Second)
	ticker2 := time.NewTicker(30 * time.Second)
	for {
		now := time.Now().Unix()
		select {
		case <-ticker1.C:
			for node, _ := range sentinel.nodeRetTime { //遍历哨兵监管的所有节点
				if _, ok := LogoffNodes[node]; ok { //如果节点存在于下线节点缓存中，说明该节点已下线不进行hb检测
					continue
				}
				ip, port := strings.Split(node, ":")[0], strings.Split(node, ":")
				resp, err := http.Get(fmt.Sprintf("http://%s:%d/hb", ip, port))
				if err != nil { //目标节点未返回alive信息，不修改最后一次确认时间
					log.Printf("HeartbeatCheck:%s", err.Error())
					return
				}
				isAlive, err := RespReader(resp)
				if err != nil {
					log.Printf("HeartbeatCheck:%s", err.Error())
					return
				}
				if string(isAlive) == "alive" { //返回并且响应报文内容为alive则修改，响应但内容错误也不会修改
					sentinel.nodeRetTime[node] = time.Now().Unix()
					return
				}
			}
		case <-ticker2.C: //恢复部分记得修改为异步恢复
			for node, tm := range sentinel.nodeRetTime {
				_, ok := LogoffNodes[node]
				if now-tm >= 60 && !ok { //超过60s且不存在于下线名单中（减少循环次数防止重复添加），未回应则认为该节点下线
					go LogOffHandler(node) //对下线节点进行下线处理
				}
			}
		}
	}
}

func LogOffHandler(node string) {
	LogoffMu.Lock()
	LogoffNodes[node] = true //将该节点加入下线缓存列表,同步的更新缓存列表为正在进行下线处理
	LogOffCache[node] = make([]int, 0)
	for slot, nd := range MySentinel.Slots {
		if nd == node {
			LogOffCache[nd] = append(LogOffCache[nd], slot)
		}
	}
	LogoffMu.Unlock()

	reassignSlots := make([]int, 0)
	runningNodes := make(map[string]struct{})
	for slot, nd := range MySentinel.Slots { //将下线节点所监管的slot收集起来待重分配
		if nd == node { //nd是当前slot监管节点，node是下线节点
			reassignSlots = append(reassignSlots, slot)
		} else {
			runningNodes[nd] = struct{}{}
		}
	} //该循环不会清除下线节点对slot的监管，因为重分配报文不一定发送成功，待发成功后再将重分配情况写入
	runningNodesNum := len(runningNodes) //当前正在运行节点数
	runningNodesArray := make([]string, 0)
	for nd := range runningNodes {
		runningNodesArray = append(runningNodesArray, nd)
	}
	perReassignSlotsNum := len(reassignSlots) / runningNodesNum //各节点所得重分配槽位数
	reAssignStatus := make(map[string][]int)

	for i := 0; i < runningNodesNum-1; i++ {
		reAssignStatus[runningNodesArray[i]] = reassignSlots[i*perReassignSlotsNum : (i+1)*perReassignSlotsNum]
	}
	reAssignStatus[runningNodesArray[runningNodesNum-1]] = reassignSlots[runningNodesNum-1:]
	sendBody := ReAssignStatus{Status: reAssignStatus}
	body, err := json.Marshal(sendBody)
	if err != nil {
		log.Fatalf("LogOffHandler MarshalError :%s", err.Error())
	}
	for _, addr := range runningNodesArray {
		ip := strings.Split(addr, ":")[0]
		port, err := strconv.Atoi(strings.Split(addr, ":")[1])
		if err != nil {
			log.Fatalf("please check your ip:port format :HeartBeatCheck.ticker2:%s", err.Error())
		}
		resp := new(http.Response)
		start := time.Now().Unix()
		for {
			resp, err = http.Post(fmt.Sprintf("http://%s:%d/reAssign", ip, port), "", strings.NewReader(string(body)))
			if err == nil {
				break
			}
			if time.Now().Unix()-start > 3 {
				log.Fatalf("reSend reAssign failed, please check your net or your running clusterNode:%s", err.Error())
			}
		}
		bytes, err := RespReader(resp)
		if err != nil {
			log.Fatalf("please check %s:%d's status:%s", ip, port, err.Error())
		}
		if string(bytes) == "ok" {
			continue
		} else {
			log.Fatalf("please check %s:%d's status: %s", ip, port, "err : target node's resp is not \"ok\" !")
		}
	}

	LogoffMu.Lock() //同步地修改本节点状态为已下线处理结束
	LogoffNodes[node] = false
	LogoffMu.Unlock()
}

func RespReader(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	isAlive, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("RespReader:%s", err.Error())
		return nil, err
	}
	return isAlive, nil
}

func mergeAddr(ip string, port int) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

func (sentinel *Sentinel) SetSentinelContent(status cluster.ClusterNode) {
	mainAddr := mergeAddr(status.Ip, status.Port)
	now := time.Now().Unix()

	sentinel.nodeRetTime[mainAddr] = now        //将主节点的计时放入哨兵
	for slot, isMonitor := range status.Slots { //将主节点的监管槽位计入哨兵
		if isMonitor == 1 {
			sentinel.Slots[slot] = mainAddr
		}
	}

	for otherAddr, otherStatus := range status.ClusterNodes { //other代表的是其他的节点
		for slot, isMonitor := range otherStatus.Slots {
			if isMonitor == 1 {
				sentinel.Slots[slot] = otherAddr
			}
		}
	}
}

func Recover() {

}
