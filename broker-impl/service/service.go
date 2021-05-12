package service

import (
	"fmt"
	"sync"
	"test/broker"
	"time"
)

type BrokerService struct {
	consumer      chan broker.Message
	lastPingMutex sync.RWMutex
	lastPing      time.Time
}

func (bs *BrokerService) GetLastPing() time.Time {
	bs.lastPingMutex.RLock()
	defer bs.lastPingMutex.RUnlock()
	return bs.lastPing
}

func (bs *BrokerService) SetLastPing(t time.Time) {
	bs.lastPingMutex.Lock()
	defer bs.lastPingMutex.Unlock()
	bs.lastPing = t
}

func (bs *BrokerService) Send(msg broker.Message) error {
	select {
	case bs.consumer <- msg:
		return nil
	default:
		return fmt.Errorf("service queue overflow")
	}
}

func (bs *BrokerService) GetConsumer() <-chan broker.Message {
	return bs.consumer
}

func NewBrokerService() *BrokerService {
	return &BrokerService{
		make(chan broker.Message, 1024),
		sync.RWMutex{},
		time.Now(),
	}
}
