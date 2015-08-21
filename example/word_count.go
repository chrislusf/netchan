package main

import (
	"flag"
	"strings"

	"github.com/chrislusf/netchan/example/flame"
)

type KeyValue struct {
	Key   string
	Value string
}

func NewKeyValue(key, value string) KeyValue {
	return KeyValue{
		Key:   key,
		Value: value,
	}
}

func main() {
	flag.Parse()

	flame.TextFile("/etc/passwd", 1).Map(func(line string, ch chan string) {
		ch <- line
	}).Filter(func(line string) bool {
		return !strings.HasPrefix(line, "#")
	}).Map(func(line string, ch chan KeyValue) {
		ch <- NewKeyValue(line[0:4], line)
	}).LocalSort(func(a string, b string) bool {
		if strings.Compare(a, b) < 0 {
			return true
		}
		return false
	}).Map(func(key, line string) string {
		println(key, line)
		return key
	}).Map(func(key string, ch chan int) {
		println("key:", key)
		/*}).Partition(func(line string) int {
		i++
		return i*/
	}).Run()

}
