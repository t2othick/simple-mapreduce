package pkg

type KeyValue struct {
	Key   string
	Value string
}

func runMasterWork(jobName string, inFile string, outFile string, nMap int, nReduce int) {
	// 将原始文件，切成 nMap 份，写回磁盘
	files := make([]string, nMap)
	// 调用 schedule, phase = map
	schedule(jobName, "map", nMap, nReduce, files)
	// 调用 schedule, phase = reduce
	schedule(jobName, "map", nMap, nReduce, files)
	// 调用 merge
}

func schedule(jobName, phase string, nMap, nReduce int, files []string) {
	// 如果 phase 是 map，轮流调用 doMap

	// 如果 phase 是 reduce，轮流调用 doReduce
}

func merge(jobName string, outFile string, nReduce int) {
	// 将所有 reducef-jobname-#reduce 合并成 outFile
}

func doMap(jobName string, taskID int, file string, nReduce int) bool {
	// 调用 mapF，将对应 mapF 的返回值，写入 mapf-jobname-#map-#reduce
	return true
}

func doReduce(jobName string, nMap int, taskID int) bool {
	// 循环读取对应 mapf-jobname-#map-#reduce 内容
	// 遍历每个 key，调用 reduceF
	// 将返回值和 key，写入 reducef-jobname-#reduce
	return true
}

func mapF(filename string, content string) []KeyValue {
	return nil
}

func reduceF(key string, keyValues []KeyValue) string {
	return ""
}
