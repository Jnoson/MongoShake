package rocketmq

import (
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"strings"
	"time"
)

var (
	topicDefault           = "mongoshake"
	topicSplitter          = "@"
	brokersSplitter        = ","
	defaultPartition int32 = 0
)

type Message struct {
	Key       []byte
	Value     []byte
	Offset    int64
	TimeStamp time.Time
}

type Config struct {
	Config *primitive.Interceptor
}


// parse the address (topic@broker1,broker2,...)
func parse(address string) (string, []string, error) {
	arr := strings.Split(address, topicSplitter)
	l := len(arr)
	if l == 0 || l > 2 {
		return "", nil, fmt.Errorf("address format error")
	}

	topic := topicDefault
	if l == 2 {
		topic = arr[0]
	}

	brokers := strings.Split(arr[l-1], brokersSplitter)
	return topic, brokers, nil
}