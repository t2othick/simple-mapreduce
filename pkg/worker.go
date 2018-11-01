package pkg

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type Worker struct {
	Host string // nfs file host, folder path if local
}

var WORKER = Worker{
	Host: Config.NfsHost,
}

func (w Worker) doMap(jobName string, taskID int, file string, nReduce int) bool {
	// 调用 mapF，将对应 mapF 的返回值，写入 mapf-jobname-#map-#reduce
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		return false
	}
	kvs := mapF(file, string(contents))

	kvsData := make(map[int][]KeyValue)
	for _, kv := range kvs {
		kvsData[reduceHash(kv.Key)%nReduce] = append(kvsData[reduceHash(kv.Key)%nReduce], kv)
	}

	for r, kvs := range kvsData {
		file, err := os.OpenFile(mapFileName(jobName, w.Host, taskID, r), os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return false
		}
		defer file.Close()
		enc := json.NewEncoder(file)
		for _, kv := range kvs {
			enc.Encode(kv)
		}
	}
	return true
}

func (w Worker) doReduce(jobName string, nMap int, taskID int) bool {
	// 循环读取对应 mapf-jobname-#map-#reduce 内容
	// 遍历每个 key，调用 reduceF
	// 将返回值和 key，写入 reducef-jobname-#reduce
	kvs := make(map[string][]KeyValue)
	for mapTaskID := 0; mapTaskID < nMap; mapTaskID ++ {
		path := mapFileName(jobName, w.Host, mapTaskID, taskID)
		file, _ := os.OpenFile(path, os.O_RDONLY, 0666)
		dec := json.NewDecoder(file)
		for dec.More() {
			kv := KeyValue{}
			dec.Decode(&kv)
			kvs[kv.Key] = append(kvs[kv.Key], kv)
		}
	}

	reducePath := reduceFileName(jobName, w.Host, taskID)
	file, _ := os.OpenFile(reducePath, os.O_CREATE|os.O_WRONLY, 0666)
	defer file.Close()
	enc := json.NewEncoder(file)
	for key, keyValues := range kvs {
		result := reduceF(key, keyValues)
		enc.Encode(KeyValue{Key: key, Value: result})
	}

	return true
}
