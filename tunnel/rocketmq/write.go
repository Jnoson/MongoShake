package rocketmq

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

type SyncWriter struct {
	nameservr  []string
	topic      string
	retrytimes int
	producer   rocketmq.Producer
}

func NewSyncWriter(address string, retryTime int) (*SyncWriter, error) {

	topic, nameserver, err := parse(address)
	if err != nil {
		return nil, err
	}

	s := &SyncWriter{
		nameservr:  nameserver,
		topic:      topic,
		retrytimes: retryTime,
	}

	return s, nil
}

func (s *SyncWriter) Start() error {
	producer, err := rocketmq.NewProducer(producer.WithNsResovler(primitive.NewPassthroughResolver(s.nameservr)), producer.WithGroupName("mongoshake"), producer.WithRetry(2))
	if err != nil {
		return err
	}
	s.producer = producer
	producer.Start()
	return nil
}

func (s *SyncWriter) SimpleWrite(input []byte) error {
	return s.send(input)
}

func (s *SyncWriter) send(input []byte) error {
	// use timestamp as key
	msg := &primitive.Message{
		Topic: s.topic,
		Body:  input,
	}
	res, err := s.producer.SendSync(context.Background(), msg)
	if err != nil {
		fmt.Printf("send message error: %s\n", err)
	} else {
		fmt.Printf("send message success: result=%s\n", res.String())
	}
	return err
}

func (s *SyncWriter) Close() error {
	return s.producer.Shutdown()
}
