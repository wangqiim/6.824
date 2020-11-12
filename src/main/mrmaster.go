package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

import "../mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}
	//a := os.Args[1:]
	//传递参数所有文件名给 (files, nReduce) *Master
	m := mr.MakeMaster(os.Args[1:], 10)
	//循环检验matser判断是否完成所有任务
	for m.Done() == false {
		time.Sleep(time.Second)
		m.TimeTick()	//每秒给一个时钟滴答
	}
	time.Sleep(time.Second)
}
