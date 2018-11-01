package main

import (
	"fmt"
	"github.com/t2othick/simple-mapreduce/pkg"
)

func main() {
	master := pkg.Master{
		Host: pkg.Config.NfsHost,
	}
	err := master.Run(
		"player-mr",
		"/Users/hdd/Desktop/data/input.txt",
		"/Users/hdd/Desktop/data/output.json",
		4,
		2)
	fmt.Println(err)
}
