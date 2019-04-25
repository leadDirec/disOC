package conf

import "github.com/leadDirec/disOC/util"

var Config = &CustomerConfig{}

func InitConfig(file string) *CustomerConfig {
	util.LoadConf(Config, file)
	return Config
}

type CustomerConfig struct {
	Registry        ConfigEtcdOption `yaml:"etcd_option"`
	Nsqd            ConfigNSQ        `yaml:"nsq"`
	QuorumCap       int              `yaml:"quorum_cap"`        //集群数量
	IsMasterConsume bool             `yaml:"is_master_consume"` //主节点是否消费数据
	SlaveConsumerTopic string `yaml:"slave.consumer.topic"`
	MasterConsumerTopic []string `yaml:"master.consumer.topic"`
}

type ConfigEtcdOption struct {
	Hosts        []string `yaml:"hosts"`
	ElectionPath string   `yaml:"election_path"`
	SlavePath    string   `yaml:"slave_path"`
}

type ConfigNSQ struct {
	LookupAddress []string `yaml:"lookup_address_list"`
	NsqdAddress   string   `yaml:"addr"`
}
