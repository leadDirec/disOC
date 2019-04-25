package nsq

import (
	"github.com/nsqio/go-nsq"
	"github.com/leadDirec/disOC/models"
	"encoding/json"
	"fmt"
	"github.com/leadDirec/disOC/util"
	"github.com/leadDirec/disOC/conf"
)

type ScheduleConsumer struct {
	Consumer     []*nsq.Consumer
}

func InitScheduleConsumer(config conf.ConfigNSQ) *ScheduleConsumer {
	sc := &ScheduleConsumer{
		Consumer:make([]*nsq.Consumer,0),
	}
	for _,t := range conf.Config.MasterConsumerTopic {
		sc.Consumer = append(sc.Consumer, util.CreateNSQConsumer(t, RealDataConsumerChannel, config.LookupAddress, new(consumer)))
	}
	return sc
}

func (self *ScheduleConsumer) Stop() {
	for _,c := range self.Consumer {
		c.Stop()
	}
}

type consumer struct {
	
}

func (c *consumer)HandleMessage(message *nsq.Message) error {
	data := new(models.Item)
	err := json.Unmarshal(message.Body, &data)
	if err != nil {
		fmt.Println("json nsq msg deserail failed**********************************************************")
		return nil
	}
	nodeCount := conf.Config.QuorumCap
	if !conf.Config.IsMasterConsume {
		nodeCount--
	}
	index := util.HashCode(data.Code) % nodeCount
	key := fmt.Sprintf(conf.Config.SlaveConsumerTopic, index)
	Producer.Publish(key, message.Body)
	return nil
}