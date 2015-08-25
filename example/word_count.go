package main

import (
	"flag"
	"strings"

	_ "github.com/chrislusf/netchan/example/driver"
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

	// test1()
	test2()

}

func test1() {
	flame.NewContext().TextFile(
		"/etc/passwd", 1,
	).Filter(func(line string) bool {
		println(line)
		return !strings.HasPrefix(line, "#")
	}).Map(func(line string, ch chan string) {
		for _, token := range strings.Split(line, ":") {
			ch <- token
		}
	}).Map(func(key string) int {
		return 1
	}).Reduce(func(x int, y int) int {
		return x + y
	}).Map(func(x int) {
		println("count:", x)
	})
}

func test2() {
	flame.NewContext().TextFile(
		"/etc/hosts", 7,
	).Partition(
		3,
	).Map(func(line string) string {
		print(".")
		return line
	}).Sort(func(a string, b string) bool {
		if strings.Compare(a, b) < 0 {
			return true
		}
		return false
	}).Map(func(line string) {
		println(line)
	})

}
