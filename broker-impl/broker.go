package broker_impl

import (
	"fmt"
	"log"
	"sync"
	"test/broker"
	"test/broker-impl/service"
	"time"
)

const responseChanCleanPeriod = time.Second * 30

var brokerTest broker.Broker = (*BrokerImpl)(nil)

type responseChan struct {
	ch      chan broker.Message
	created time.Time
}

type BrokerImpl struct {
	servicesMutex sync.RWMutex
	services      map[string]*service.BrokerService

	respChansMutex sync.RWMutex
	respChans      map[uint64]responseChan

	activeWorkers int
	toSend        chan broker.Message
	toRespond     chan broker.Message
	pings         chan string
	stop          chan chan struct{}
}

func NewBrokerImpl() *BrokerImpl {
	return &BrokerImpl{
		sync.RWMutex{},
		make(map[string]*service.BrokerService),
		sync.RWMutex{},
		make(map[uint64]responseChan),

		0,
		make(chan broker.Message, 1024),
		make(chan broker.Message, 1024),
		make(chan string, 1024),
		make(chan chan struct{}),
	}
}

func (b *BrokerImpl) Start() {
	b.activeWorkers = 5
	go b.cleaner()
	go b.pingHandler()
	go b.sender()
	go b.responder()
	go b.responseChanCleaner()
}

func (b *BrokerImpl) cleaner() {
	t := time.NewTicker(broker.ServiceTimeout)
	for {
		select {
		case <-t.C:
			b.dropInactiveServices()
		case ch := <-b.stop:
			close(ch)
			return
		}
	}
}

func (b *BrokerImpl) responseChanCleaner() {
	t := time.NewTicker(responseChanCleanPeriod)
	for {
		select {
		case <-t.C:
			b.respChansMutex.Lock()
			for k, v := range b.respChans {
				if v.created.Add(responseChanCleanPeriod).Before(time.Now()) {
					delete(b.respChans, k)
					close(v.ch)
				}
			}
			b.respChansMutex.Unlock()
		case ch := <-b.stop:
			close(ch)
			return
		}
	}
}

func (b *BrokerImpl) dropInactiveServices() {
	b.servicesMutex.Lock()
	defer b.servicesMutex.Unlock()
	for name, service := range b.services {
		if service.GetLastPing().Add(broker.ServiceTimeout).Before(time.Now()) {
			delete(b.services, name)
			log.Printf("service %s dropped", name)
		}
	}
}

func (b *BrokerImpl) pingHandler() {
	for {
		select {
		case name := <-b.pings:
			b.servicesMutex.RLock()
			service, ok := b.services[name]
			b.servicesMutex.RUnlock()
			if ok {
				service.SetLastPing(time.Now())
				log.Printf("received ping from %s", name)
			}
		case ch := <-b.stop:
			close(ch)
			return
		}
	}
}

func (b *BrokerImpl) sender() {
	for {
		select {
		case msg := <-b.toSend:
			if msg.MessageType == broker.TypeDirectMessage {
				b.servicesMutex.RLock()
				service, ok := b.services[msg.To]
				b.servicesMutex.RUnlock()
				if ok {
					service.Send(msg)
				}
			} else if msg.MessageType == broker.TypeBroadcastMessage {
				for _, v := range b.getServices() {
					v.Send(msg)
				}
			}
		case ch := <-b.stop:
			close(ch)
			return
		}
	}
}

func (b *BrokerImpl) responder() {
	for {
		select {
		case msg := <-b.toRespond:
			if msg.MessageType == broker.TypeDirectResponse {
				b.respChansMutex.Lock()
				rc := b.respChans[msg.ID]
				rc.ch <- msg
				delete(b.respChans, msg.ID)
				b.respChansMutex.Unlock()
				log.Printf("responded from %s to %s", msg.From, msg.To)
			}
		case ch := <-b.stop:
			close(ch)
			return
		}
	}
}

func (b *BrokerImpl) Stop() {
	for i := 0; i < b.activeWorkers; i++ {
		ch := make(chan struct{})
		b.stop <- ch
		<-ch
	}
	close(b.pings)
	close(b.toRespond)
	close(b.toSend)
}

func (b *BrokerImpl) Register(name string) (<-chan broker.Message, error) {
	b.servicesMutex.Lock()
	defer b.servicesMutex.Unlock()
	_, ok := b.services[name]
	if ok {
		return nil, fmt.Errorf("service already registered")
	}
	service := service.NewBrokerService()
	b.services[name] = service
	return service.GetConsumer(), nil
}

func (b *BrokerImpl) Broadcast(msg broker.Message) error {
	if msg.MessageType != broker.TypeBroadcastMessage {
		return fmt.Errorf("expected broadcast message type")
	}
	select {
	case b.toSend <- msg:
		log.Printf("received broadcast from %s", msg.From)
		return nil
	default:
		return fmt.Errorf("send queue overflow")
	}
}

func (b *BrokerImpl) handlePing(serviceFrom string) error {
	select {
	case b.pings <- serviceFrom:
		return nil
	default:
		return fmt.Errorf("pings queue overflow")
	}
}

func (b *BrokerImpl) handleDirect(msg broker.Message) (chan broker.Message, error) {
	respChan := make(chan broker.Message, 1)
	select {
	case b.toSend <- msg:
		b.respChansMutex.Lock()
		b.respChans[msg.ID] = responseChan{respChan, time.Now()}
		b.respChansMutex.Unlock()
		log.Printf("send message from %s to %s", msg.From, msg.To)
	default:
		return nil, fmt.Errorf("send queue overflow")
	}
	return respChan, nil
}

func (b *BrokerImpl) Send(msg broker.Message) (chan broker.Message, error) {
	if msg.MessageType == broker.TypePing {
		return nil, b.handlePing(msg.From)
	} else if msg.MessageType == broker.TypeDirectMessage {
		return b.handleDirect(msg)
	} else {
		return nil, fmt.Errorf("expected ping or direct message type")
	}
}

func (b *BrokerImpl) Response(msg broker.Message) error {
	if msg.MessageType == broker.TypeDirectResponse {
		b.toRespond <- msg
	} else {
		return fmt.Errorf("expected direct response type")
	}
	return nil
}

func (b *BrokerImpl) GetServices() []string {
	b.servicesMutex.RLock()
	services := make([]string, len(b.services))
	for name := range b.services {
		services = append(services, name)
	}
	b.servicesMutex.RUnlock()
	return services
}

func (b *BrokerImpl) getServices() []*service.BrokerService {
	b.servicesMutex.RLock()
	services := make([]*service.BrokerService, len(b.services))
	for _, service := range b.services {
		services = append(services, service)
	}
	b.servicesMutex.RUnlock()
	return services
}
