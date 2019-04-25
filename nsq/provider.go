package nsq

import (
	"github.com/nsqio/go-nsq"
	"github.com/leadDirec/disOC/conf"
	"github.com/leadDirec/disOC/util"
)

var (
	Producer *nsq.Producer
)

func InitNsqdProducer(nsqConf conf.ConfigNSQ) {
	Producer = util.CreateProducer(nsqConf.NsqdAddress)
}
