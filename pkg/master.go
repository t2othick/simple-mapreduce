package pkg

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
)

type Master struct {
	Host string // nfs file host, folder path if local
}

func (m Master) Run(jobName string, inFile string, outFile string, nMap int, nReduce int) error {
	// 将原始文件，切成 nMap 份，写回磁盘
	files := splitFiles(jobName, m.Host, inFile, nMap)
	// 调用 schedule, phase = map
	m.schedule(jobName, "map", nMap, nReduce, files)
	// 调用 schedule, phase = reduce
	m.schedule(jobName, "reduce", nMap, nReduce, files)
	// 调用 merge
	m.doMerge(jobName, outFile, nReduce)

	// 清理临时文件
	clearDir(m.Host)

	return nil
}

func (m Master) schedule(jobName, phase string, nMap, nReduce int, files []string) {
	// 如果 phase 是 map，轮流调用 doMap
	if phase == "map" {
		for taskID := 0; taskID < nMap; taskID ++ {
			if success := WORKER.doMap(jobName, taskID, files[taskID], nReduce); !success {
				panic(errors.New(fmt.Sprintf("do map task %d failed", taskID)))
			}
			fmt.Println(fmt.Sprintf("do map task %d ok", taskID))
		}
	}

	if phase == "reduce" {
		for taskID := 0; taskID < nReduce; taskID ++ {
			if success := WORKER.doReduce(jobName, nMap, taskID); !success {
				panic(errors.New(fmt.Sprintf("do reduce task %d failed", taskID)))
			}
			fmt.Println(fmt.Sprintf("do redece task %d ok", taskID))
		}
	}

}

func (m Master) doMerge(jobName string, outFile string, nReduce int) {
	// 将所有 reducef-jobname-#reduce 合并成 outFile
	outFD, _ := os.OpenFile(outFile, os.O_CREATE|os.O_WRONLY, 0666)
	for reduceID := 0; reduceID < nReduce; reduceID ++ {
		reduceFD, _ := os.OpenFile(reduceFileName(jobName, m.Host, reduceID), os.O_RDONLY, 0666)
		data, _ := ioutil.ReadAll(reduceFD)
		fmt.Print(string(data))
		outFD.Write(data)
	}
}
