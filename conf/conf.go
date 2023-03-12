package conf

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

var YamlConfig Config

type Config struct {
	IP           string   `yaml:"ip"`
	Port         int      `yaml:"port"`
	ClusterStart int      `yaml:"clusterStart"`
	ClusterEnd   int      `yaml:"clusterEnd"`
	Peers        []string `yaml:"peers"`
}

func SetConfig() {
	yamlFile, err := ioutil.ReadFile("./conf/conf.yaml")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	err = yaml.Unmarshal(yamlFile, &YamlConfig)
	if err != nil {
		log.Fatalln("unmarshal yaml file failed")
		return
	}
}
