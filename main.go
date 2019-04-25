package main

import (
	"github.com/leadDirec/disOC/conf"
	"github.com/leadDirec/disOC/registry"
)

func main() {
	file := "conf/disOC.yaml"
	cnf := conf.InitConfig(file)
	service, _ := registry.NewService(cnf.Registry.Hosts, cnf.Registry.SlavePath, cnf.Registry.ElectionPath, cnf.QuorumCap, cnf.IsMasterConsume)
	service.Start()
}