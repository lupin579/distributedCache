package tmpTest

import (
	"encoding/json"
	"log"
	"testing"
)

func TestReOnlineNotice(t *testing.T) {
	tmp := make(map[string][]int)
	tmp["asd"] = []int{231, 123, 456}
	tmp["sd"] = []int{231, 123, 456}
	tmp["d"] = []int{231, 123, 456}
	bytes, err := json.Marshal(tmp)
	if err != nil {
		log.Fatalln(err.Error())
	}
	println(string(bytes))
}
