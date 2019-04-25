package nsq

import (
	"github.com/nsqio/go-nsq"
	"fmt"
	"github.com/leadDirec/disOC/util"
	"github.com/leadDirec/disOC/conf"
)

type RealQueueConsumer struct {
	index    int
	topic    string
	consumer *nsq.Consumer
}

func InitRealQueueConsumer(index int, nsq_config conf.ConfigNSQ) *RealQueueConsumer {
	consumer := new(RealQueueConsumer)
	consumer.topic = fmt.Sprintf(conf.Config.SlaveConsumerTopic, index)
	consumer.consumer = util.CreateNSQConsumer(consumer.topic, RealDataConsumerChannel, nsq_config.LookupAddress, consumer)
	fmt.Println("start consume real::::", consumer.topic)
	return consumer
}

func (self *RealQueueConsumer) HandleMessage(message *nsq.Message) error {
	//handle messag
	//TODO 业务逻辑 多线程处理即可
	return nil
}

func (self *RealQueueConsumer) Stop() {
	self.consumer.Stop()
	fmt.Println("stop consume real::::", self.topic)
}
