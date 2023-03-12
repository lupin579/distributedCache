package model

import "time"

type Node struct {
	Specific  bool   `json:"specific"` //true头尾节点，false普通节点
	Key       string `json:"key"`
	Value     string `json:"value"`
	Prev      *Node  `json:"prev"`
	Next      *Node  `json:"next"`
	ExpiresAt int64  `json:"expiresAt"`
}

func (nd *Node) GetNodeExpires() int64 {
	return nd.ExpiresAt
}

func (nd *Node) GetNodeKey() string {
	return nd.Key
}

func (nd *Node) GetNodeValue() string {
	return nd.Value
}

/*
node是否过期
*/
func (nd *Node) IsExpired() bool {
	if nd.ExpiresAt <= time.Now().Unix() {
		return true
	}
	return false
}
