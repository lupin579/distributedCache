package cluster

import (
	"distributedCache/conf"
	"distributedCache/model"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*
MyClusterNode
初始化顺序：
全局变量在程序初始化时就已经加载完毕了，
但是这时conf.YamlConfig中的数据还没有读取进来，
因为这时还没有执行main中的config读取函数
*/
var MyClusterNode = ClusterNode{
	Kv:           make(map[string]*model.Node),
	ClusterNodes: make(map[string]ClusterNode),
}

var MyBlackList BlackList

type BlackList struct {
	List     map[string]int64 //stirng代表穿透的key，int64代表设置的过期时间
	EditTime int64
}

type ClusterNode struct {
	mu           sync.Mutex
	Slots        [64]uint8              `json:"Slots"`        //负责的槽位置1
	Kv           map[string]*model.Node `json:"Kv"`           //本节点中存储的Kv,ClusterNodes中的此属性为空（即不会记录其他节点所存储的Kv值）
	ClusterNodes map[string]ClusterNode `json:"ClusterNodes"` //本节点眼中的集群各节点的情况
	Ip           string                 `json:"Ip"`
	Port         int                    `json:"Port"`
}

/*
初始化节点和黑名单
*/
func ClusterNodeInit(slots []int) {
	MyBlackList = BlackList{
		List:     make(map[string]int64),
		EditTime: 0,
	}
	if len(slots) == 0 {
		for i := conf.YamlConfig.ClusterStart; i <= conf.YamlConfig.ClusterEnd; i++ {
			MyClusterNode.Slots[i] = 1
		}
	} else {
		for i := range slots {
			MyClusterNode.Slots[i] = 1
		}
	}
	MyClusterNode.Ip = conf.YamlConfig.IP
	MyClusterNode.Port = conf.YamlConfig.Port
	for _, peer := range conf.YamlConfig.Peers {
		portNum, err := strconv.Atoi(strings.Split(peer, ":")[1])
		if err != nil {
			panic("ClusterNodeInit failed : string strconv to int")
		}
		MyClusterNode.ClusterNodes[peer] = ClusterNode{
			Slots:        [64]uint8{},
			Kv:           nil, //不存其他节点的Kv
			ClusterNodes: nil, //初始化时不写入，meet时写入
			Ip:           strings.Split(peer, ":")[0],
			Port:         portNum,
		}
	}
}

func (this *BlackList) Append(key string) {
	this.List[key] = time.Now().Add(time.Second * 30).Unix()
}

func (this *BlackList) Query(key string) bool {
	if _, exist := this.List[key]; exist {
		this.List[key] = time.Now().Add(30 * time.Second).Unix() //存在则更新存在时长
		return exist
	} else { //不存在则说明mysql中有数据或者是第一次请求该key，直接返回不存在即可
		return exist
	}
}

func (this *ClusterNode) GetSlots() [64]uint8 {
	return this.Slots
}

func (this *ClusterNode) GetMyClusterIP() string {
	return this.Ip
}

func (this *ClusterNode) GetMyClusterPort() int {
	return this.Port
}

func (this *ClusterNode) SetKV(key string, nd *model.Node) {
	this.Kv[key] = nd
}

func (this *ClusterNode) AppendPeer(Ip string, Port int, start, end string) error {
	slotsStart, err := strconv.Atoi(start)
	if err != nil {
		log.Fatalln("end atoi failed")
		return err
	}
	slotsEnd, err := strconv.Atoi(end)
	if err != nil {
		log.Fatalln("end atoi failed")
		return err
	}
	var Slots [64]uint8
	for i := slotsStart; i <= slotsEnd; i++ {
		Slots[i] = 1
	}
	MyClusterNode.mu.Lock()
	MyClusterNode.ClusterNodes[fmt.Sprintf("%s:%d", Ip, Port)] = ClusterNode{
		Slots: Slots,
		Ip:    Ip,
		Port:  Port,
	}
	MyClusterNode.mu.Unlock()
	return nil
}

func (this *ClusterNode) WhoHasThisSlot(pos uint32) (Ip string, Port int) {
	for _, node := range this.ClusterNodes {
		if node.Slots[pos] == 1 {
			return node.Ip, node.Port
		}
	}
	return this.Ip, this.Port
}

/*
根据yaml中的peers来，
发送meet请求给目标服务器，
收到请求后根据请求（成功与否）判断是否将该peer放入clusterNode的peers（ClusterNodes）中
*/

func (this *ClusterNode) CleanDeletedNode() { //因为lru会定期删除过期节点，所以要定期查看cluster中的Kv是否有node（v）被删掉了
	count := 0
	for k, nd := range this.Kv { //每次执行该函数随机查询5个
		if count >= 5 {
			break
		}
		if nd == nil {
			delete(this.Kv, k)
		}
		count++
	}
}
