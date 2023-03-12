package myHttp

import (
	"distributedCache/cluster"
	"distributedCache/conf"
	"distributedCache/lru"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
)

func SendCacheTo(ip string, port int, key string) (string, error) {
	fmtStr := fmt.Sprintf("%s", key)
	body := strings.NewReader(fmtStr)
	resp, err := http.Post(fmt.Sprintf("http://%s:%d/redirect", ip, port), "text/plain", body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", errors.New("read body failed")
	}
	return string(respBytes), nil
}

func SetPeerStatus() error {
	if len(conf.YamlConfig.Peers) == 0 {
		return nil
	}
	for _, peer := range conf.YamlConfig.Peers {
		ip := strings.Split(peer, ":")[0]
		sport := strings.Split(peer, ":")[1]
		port, err := strconv.Atoi(sport)
		if err != nil {
			log.Fatalln("SetPeerStatus Atoi failed")
			return err
		}
		err = Meet(ip, port)
		if err != nil {
			log.Fatalln("myHTTP Meet failed")
			return err
		}
	}
	return nil
}

func SelectCluster(key string, pos uint32) (string, error) {
	mySlots := cluster.MyClusterNode.GetSlots()
	if mySlots[pos] == 1 { //槽位为本地管理
		value := lru.LruCache.Get(key)
		if value != "-1" { //本地redis中有
			return fmt.Sprintf("redis:%s", value), nil
		} else { //本地redis中没有
			if cluster.MyBlackList.Query(key) { //若存在于黑名单中，则直接返回不再查询mysql，并刷新这个key的过期时间防止恶意穿透
				return "that value is not in mysql too", nil
			}
			value = strconv.Itoa(rand.Int()) //当本key不存在与黑名单中时，再模拟从mysql读出
			if len(value) == 3 {             //模拟mysql中也没有这条数据，一定是第一次请求该key，如果是第二次请求则已经被加入黑名单了
				cluster.MyBlackList.Append(key) //将数据加入黑名单，过期时间为30s
				return "that value is not in mysql too", nil
			} else { //模拟mysql中有这条数据
				nd := lru.LruCache.Put(key, value, 30)           //放入lru链表
				cluster.MyClusterNode.SetKV(nd.GetNodeKey(), nd) //放入clusterNode链表
				return fmt.Sprintf("mysql:%s", value), nil
			}
		}
	} else { //槽位不是本地管理
		ip, port := cluster.MyClusterNode.WhoHasThisSlot(pos)
		value, err := SendCacheTo(ip, port, key)
		if err != nil {
			log.Fatalf("%s\n", err.Error())
			return "", errors.New("SendCacheTo failed")
		}
		return fmt.Sprintf("redirect to %s:%d , %s", ip, port, value), nil
	}
}

/*
Meet
req格式：ip:port:start:end
resp格式：ip:port:slotStart:slotEnd
*/
func Meet(ip string, port int) error {
	fileReader := strings.NewReader(fmt.Sprintf("%s:%d:%d:%d", cluster.MyClusterNode.GetMyClusterIP(), cluster.MyClusterNode.GetMyClusterPort(), conf.YamlConfig.ClusterStart, conf.YamlConfig.ClusterEnd))
	//req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:%d/meet", ip, port), fileReader)
	resp, err := http.Post(fmt.Sprintf("http://%s:%d/meet", ip, port), "application/x-www-form-urlencoded", fileReader)
	if err != nil {
		log.Fatalf("http resp err:%s\n", err.Error())
		return err
	}
	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln("io read failed")
		return err
	}
	if string(bodyBytes) == "error" {
		log.Fatalln("peer has some errors")
		return err
	}
	status := strings.Split(string(bodyBytes), ":")
	respPort, err := strconv.Atoi(status[1])
	if err != nil {
		log.Fatalln("port atoi failed")
		return err
	}
	cluster.MyClusterNode.AppendPeer(status[0], respPort, status[2], status[3])
	return nil
}
