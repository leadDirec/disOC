package registry

import (
	"github.com/coreos/etcd/clientv3"
	"time"
	"log"
	"sync/atomic"
	"strconv"
	"context"
	"github.com/leadDirec/disOC/util"
	"fmt"
	"encoding/json"
	"errors"
	"github.com/leadDirec/disOC/nsq"
	"github.com/leadDirec/disOC/conf"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pborman/uuid"
	"strings"
)

const (
	serviceRegistryPrefix = "service"
)

type Service struct {
	IsMasterConsume bool
	QuorumCap        int //集群数量
	Master           *atomic.Value
	Name             string
	stop             chan error
	leaseid          clientv3.LeaseID
	client           *clientv3.Client
	clusterId        uint64
	memberId         uint64
	ServicePath      string
	ElectionPath     string
	watchNodesChan   chan struct{}
	ChildrenNodeData *atomic.Value //订阅某个队列的数据
	RealConsumers    map[int]*nsq.RealQueueConsumer
	Scheduler *nsq.ScheduleConsumer
}

func NewService(endpoints []string,servicePath string, electPath string, quormCap int, isMasterConsume bool) (*Service, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
		panic(err)
	}
	service := &Service{
		IsMasterConsume:  isMasterConsume,
		QuorumCap:quormCap,
		Master:new(atomic.Value),
		stop:             make(chan error),
		client:           cli,
		ServicePath:      servicePath,
		ElectionPath:     electPath,
		watchNodesChan:   make(chan struct{}),
		ChildrenNodeData: new(atomic.Value),
		RealConsumers:    make(map[int]*nsq.RealQueueConsumer),
	}
	service.Master.Store(false)
	service.Grant() //续约
	service.Name = servicePath + "/" + serviceRegistryPrefix + "#" + util.GetInternal() + "#" + strconv.FormatInt(int64(service.leaseid), 10)
	service.WatchSelfNodesData()
	go service.Election()//竞选
	return service, nil

}

func (s *Service) Start() error {
	ch, err := s.keepAlive(s.Name, s.leaseid)
	if err != nil {
		log.Fatal(err)
		return err
	}
	for {
		select {
		case err := <-s.stop:
			s.revoke()
			return err
		case <-s.client.Ctx().Done():
			return errors.New("server closed")
		case ka, ok := <-ch:
			if !ok {
				fmt.Println("keep alive channel closed")
				s.revoke()
				return nil
			} else {
				fmt.Println("reply from etcd: %s, ttl:%d ", s.Name, ka.TTL, s.leaseid, ka.ID, ka.ClusterId, ka.MemberId, ka.Revision)
			}
		}
	}
}

func (s *Service) keepAlive(key string, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	_, err := s.client.Put(context.TODO(), key, string([]byte{}), clientv3.WithLease(id))
	if err != nil {
		log.Fatal(err)
		panic(err)
	}
	return s.client.KeepAlive(context.TODO(), id)
}

func (s *Service) revoke() error {
	//revoke 废除
	_, err := s.client.Revoke(context.TODO(), s.leaseid)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("servide:%s stop\n", s.Name)
	return err
}

func (s *Service) Grant() {
	resp, err := s.client.Grant(context.TODO(), 2)
	if err != nil {
		log.Fatal(err)
		panic(err)
	}
	s.leaseid = resp.ID
	s.clusterId = resp.ClusterId
	s.memberId = resp.MemberId
}

func (s *Service) Stop() {
	s.stop <- nil
}

func (s *Service) WatchSelfNodesData() {
	rch := s.client.Watch(context.Background(), s.Name, clientv3.WithKeysOnly())
	go func() {
		for wresp := range rch {
			for _, ev := range wresp.Events {
				switch ev.Type {
				case clientv3.EventTypePut:
					fmt.Println("watch node data put ", ev.IsModify(), string(ev.Kv.Key), string(ev.Kv.Value))
					if ev.IsModify() {
						if ev.Kv == nil || len(ev.Kv.Value) == 0 {
							continue
						}
						data := make([]string, 0)
						err := json.Unmarshal(ev.Kv.Value, &data)
						if err != nil {
							fmt.Println("node data deserial occured err::", err)
							continue
						}
						value := s.ChildrenNodeData.Load()
						fmt.Println("node data deserial success")
						fmt.Println("之前监控的节点为:", value)
						fmt.Println("目前新监控的节点为:", data)

						s.ChildrenNodeData.Store(data)
						if value == nil {
							//全量消费数据
							s.startConsume(data)
						} else {
							oldIndexes := value.([]string)
							newIndexMap := make(map[string]bool, 0)
							for _, v := range data {
								newIndexMap[v] = true
							}
							for _, v := range oldIndexes {
								if _, ok := newIndexMap[v]; !ok {
									s.stopConsume(v)
								}
							}
							s.startConsume(data)
						}
					}
				case clientv3.EventTypeDelete:
					fmt.Println("watch node data delete ", ev.IsModify(), string(ev.Kv.Key), string(ev.Kv.Value))
					if s.Master.Load().(bool) {
						s.Master.Store(false)
						s.Scheduler.Stop()
						s.watchNodesChan <- struct{}{}
						go s.Election()
					}
					s.client.Put(context.TODO(), s.Name, string([]byte{}), clientv3.WithLease(s.leaseid))
				}
			}
		}
	}()
}

//开始消费消息
func (s *Service) startConsume(indexes []string) {
	for _, index := range indexes {
		index, _ := strconv.Atoi(index)
		if _, ok := s.RealConsumers[index]; !ok {
			s.RealConsumers[index] = nsq.InitRealQueueConsumer(index, conf.Config.Nsqd)
		}
	}
}

//停止消费消息
func (s *Service) stopConsume(indexNode string) {
	index, _ := strconv.Atoi(indexNode)
	if consumer, ok := s.RealConsumers[index]; ok {
		consumer.Stop()
		delete(s.RealConsumers, index)
	}
}

//leader竞选
func (s *Service) Election() {
	s1, err := concurrency.NewSession(s.client, concurrency.WithTTL(2))
	if err != nil {
		fmt.Println("new session:", err)
		panic(err)
	}
	e1 := concurrency.NewElection(s1, s.ElectionPath)
	if err := e1.Campaign(context.Background(), util.GetInternal()+"_"+uuid.New()); err != nil {
		fmt.Println("campaign :", err)
		panic(err)
	}
	cctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	fmt.Println("elect success info,", (<-e1.Observe(cctx)))
	s.Master.Store(true)
	fmt.Println("当前节点成功竞选称为master：：：", s.Name)
	value := s.ChildrenNodeData.Load() //可能是由slave节点转成master节点的
	if value != nil {
		indexes := value.([]string)
		if len(indexes) > 0 {
			for _, index := range indexes {
				s.stopConsume(index)
			}
		}
	}
	s.Scheduler = nsq.InitScheduleConsumer(conf.Config.Nsqd)
	s.WatchNodes()
}

func (s *Service) WatchNodes() {
	rch := s.client.Watch(context.Background(), s.ServicePath, clientv3.WithPrefix())
	s.distributedNodeJob()
	go func() {
		for {
			select {
			case wresp := <-rch:
				for _, ev := range wresp.Events {
					switch ev.Type {
					case clientv3.EventTypePut:
						fmt.Println("watch node put ", ev.IsCreate(), string(ev.Kv.Key), string(ev.Kv.Value))
						if ev.IsCreate() {
							s.distributedNodeJob()
						}
					case clientv3.EventTypeDelete:
						fmt.Println("watch node delete  ", string(ev.Kv.Key), string(ev.Kv.Value))
						if s.Master.Load().(bool) && string(ev.Kv.Key) != s.Name {
							s.distributedNodeJob()
						}
					}
				}
			case <-s.watchNodesChan:
				s.Scheduler.Stop()
				return
			}
		}
	}()
}

func (s *Service) distributedNodeJob() {
	//重新分发
	keys, err := s.getNodeInfo()
	fmt.Println("keys:::", keys)
	if err != nil {
		fmt.Println("get node info occured error,", err)
		return
	}
	//主节点数据分发,
	data := s.AssignNode(keys)
	s.LoadNodeToEtcd(data)
}

//获取集群信息
func (s *Service) getNodeInfo() ([]string, error) {
	response, err := s.client.Get(context.Background(), s.ServicePath, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend))
	if err != nil {
		fmt.Println("err::", err)
		return nil, err
	}
	//重新分发
	if response == nil || response.Kvs == nil || len(response.Kvs) == 0 {
		return nil, errors.New("not found")
	}
	resp := make([]string, 0, len(response.Kvs))
	for _, v := range response.Kvs {
		resp = append(resp, string(v.Key))
	}
	return resp, nil
}


//指定节点消费索引数据
func (s *Service) AssignNode(keys []string) map[string][]string {
	distributed := make(map[string][]string)
	quorumCap := s.QuorumCap
	if !s.IsMasterConsume {
		quorumCap--
	}
	s.assignNode(quorumCap, keys, distributed)
	return distributed
}

func (s *Service) assignNode(quorumCap int, keys []string, distributed map[string][]string) {
	if quorumCap == 0 {
		return
	}
	if s.IsMasterConsume  && len(keys) == 0{
			return
	} else {
		if len(keys) == 1 {
			return
		}
	}
	for _, path := range keys {
		val := s.Master.Load()
		if val == nil {
			fmt.Println("还没有master，暂时不分发数据，略过...........................")
			return
		}
		masterNode := val.(bool)
		if masterNode && !s.IsMasterConsume {
			fmt.Println("主节点不消费数据，略过...........................")
			continue
		}
		distributed[path] = append(distributed[path], strconv.Itoa(quorumCap-1))
		quorumCap--
		if quorumCap <= 0 {
			return
		}
	}
	s.assignNode(quorumCap, keys, distributed)
}

func (s *Service) LoadNodeToEtcd(data map[string][]string) {
	for key, value := range data {
		fmt.Printf("set node data %s -- %v \n", key, value)
		s.SetData(key, value)
	}
}

//分布式锁单节点分发的话 需要保留leaseId 绑定在ttl里
func (s *Service) SetData(path string, data interface{}) {
	pathArray := strings.Split(path, "#")
	leaseId, err := strconv.ParseInt(pathArray[2], 10, 64)
	//设置1秒超时，访问etcd有超时控制
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	v,_ := json.Marshal(data)
	_, err = s.client.Put(ctx, path, string(v), clientv3.WithLease(clientv3.LeaseID(leaseId)))
	cancel()
	if err != nil {
		fmt.Println("put failed, err:", err)
		return
	}
}