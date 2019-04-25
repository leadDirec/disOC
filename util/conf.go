package util

import (
	"fmt"
	"github.com/jinzhu/configor"
)

func LoadConf(dest interface{}, path string) {
	err := configor.Load(dest, path)
	if err != nil {
		msg := "Failed to load config file !!!"
		fmt.Println("load conf occured error:", err, msg)
		panic(msg)
	}
}
