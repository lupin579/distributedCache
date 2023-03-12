package hash

import (
	"crypto/md5"
	"fmt"
	"strconv"
)

func GetHash(key string) (uint32, error) {
	instance := md5.New()
	instance.Write([]byte(key))
	hexBytes := instance.Sum(nil)
	hexStr := fmt.Sprintf("%x", hexBytes[len(hexBytes)-4:]) //取十六进制的低8位，即32位
	println(hexStr)
	hash, err := strconv.ParseUint(hexStr, 16, 32)
	if err != nil {
		return 0, err
	}
	return uint32(hash), nil
}
