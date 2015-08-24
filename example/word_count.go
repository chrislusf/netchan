package main

import (
	"flag"
	"os"
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

	println("command is:", os.Args[0])

	flame.NewContext().TextFile(
		"/etc/passwd", 1,
	).Filter(func(line string) bool {
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

	test2()

}

func test2() {
	flame.NewContext().TextFile(
		"/etc/hosts", 3,
	).Partition(
		3,
	).Sort(func(a string, b string) bool {
		if strings.Compare(a, b) < 0 {
			return true
		}
		return false
	}).Map(func(line string) {
		println(line)
	})

}
