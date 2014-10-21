#简介
基于redigo，使用pipeline强化的redis驱动

#安装
go get github.com/nybuxtsui/redis

#特性
1. 自动重连
2. 主从切换
3. 使用pipe发送指令

#使用
```
package main

import (
	"github.com/nybuxtsui/redis"
	"fmt"
)
func main() {
	var pool = redis.NewPool([]string{"127.0.0.1:6379"}, "")
	var result, err = pool.Exec("get", "__a")
	fmt.Println(result, err)
}
```
