package pkg

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
)

type KeyValue struct {
	Key   string
	Value string
}

func mapFileName(jobName string, host string, mapID int, reduceID int) string {
	filename := fmt.Sprintf("mapf-%s-%d-%d.json", jobName, mapID, reduceID)
	return filepath.Join(host, filename)
}

func reduceFileName(jobName string, host string, reduceID int) string {
	filename := fmt.Sprintf("reducef-%s-%d.json", jobName, reduceID)
	return filepath.Join(host, filename)
}

func countFileLines(fileName string) (int, error) {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	fd := bufio.NewReader(file)
	lines := 0
	for {
		_, err := fd.ReadString('\n')
		if err != nil {
			break
		}
		lines++
	}
	return lines, nil
}

func splitFileName(jobName string, host string, taskID int) string {
	filename := fmt.Sprintf("inputf-%s-%d.json", jobName, taskID)
	return filepath.Join(host, filename)
}

func splitFiles(jobName string, host string, inFile string, count int) (files []string) {

	linesNum, err := countFileLines(inFile)
	if err != nil {
		return nil
	}

	eachFileLines := linesNum / count

	file, err := os.Open(inFile)
	if err != nil {
		return nil
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for taskID := 0; taskID < count; taskID ++ {
		fileName := splitFileName(jobName, host, taskID)
		files = append(files, fileName)
		file, _ := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		defer file.Close()
		currentLines := 0
		for scanner.Scan() {
			file.Write([]byte(scanner.Text() + "\n"))
			currentLines ++
			if currentLines%eachFileLines == 0 && taskID != (count-1) {
				break
			}
		}
	}
	return files
}

func clearDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}

func reduceHash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
