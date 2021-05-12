package geo

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"test/broker"
	"test/proto"
	"time"
)

type XY struct {
	X int
	Y int
}

type GeoService struct {
	name       string
	locationMu sync.RWMutex
	location   XY

	activeWorkers int
	stop          chan chan struct{}
	broker        broker.Broker
	producer      <-chan broker.Message
}

func NewGeoService() *GeoService {
	service := &GeoService{
		randomGeoName(),
		sync.RWMutex{},
		randomXY(),

		0, make(chan chan struct{}),
		nil, nil,
	}
	service.PrintInfo()
	return service
}

func (gs *GeoService) PrintInfo() {
	gs.locationMu.RLock()
	defer gs.locationMu.RUnlock()
	log.Printf("Geo service %s location is %v", gs.name, gs.location)
}

func (gs *GeoService) GetName() string {
	return gs.name
}

func (gs *GeoService) UpdateLocation() {
	gs.locationMu.Lock()
	gs.location = randomXY()
	gs.locationMu.Unlock()
	gs.PrintInfo()
}

func (gs *GeoService) Distance(point XY) float64 {
	gs.locationMu.RLock()
	defer gs.locationMu.RUnlock()
	return math.Sqrt(math.Pow(float64(gs.location.X-point.X), 2) - math.Pow(float64(gs.location.Y-point.Y), 2))
}

func (gs *GeoService) Start(b broker.Broker, producer <-chan broker.Message) {
	gs.broker = b
	gs.producer = producer
	gs.activeWorkers = 2
	go gs.pinger()
	go gs.receiver()
}

func (gs *GeoService) pinger() {
	t := time.NewTicker(broker.ServicePingInterval)
	for {
		select {
		case <-t.C:
			if gs.broker != nil {
				_, err := gs.broker.Send(broker.Message{MessageType: broker.TypePing, From: gs.GetName()})
				if err != nil {
					log.Println(err)
				}
			}
		case ch := <-gs.stop:
			close(ch)
			return
		}
	}
}

func (gs *GeoService) receiver() {
	for {
		select {
		case msg := <-gs.producer:
			_, ok := msg.Payload.(proto.UpdateLocationRequest)
			if ok {
				gs.UpdateLocation()
				continue
			}
			req, ok := msg.Payload.(proto.GetDistanceRequest)
			if ok {
				dist := gs.Distance(XY{req.PointX, req.PointY})
				err := gs.broker.Response(broker.Message{
					MessageType: broker.TypeDirectResponse,
					From:        gs.GetName(),
					To:          msg.From,
					Payload:     proto.GetDistanceResponse{Distance: &dist},
					ID:          msg.ID,
				})
				if err != nil {
					log.Println(err)
				}
			}
		case ch := <-gs.stop:
			close(ch)
			return
		}
	}
}

func (gs *GeoService) Stop() {
	for i := 0; i < gs.activeWorkers; i++ {
		ch := make(chan struct{})
		gs.stop <- ch
		<-ch
	}
}

func randomGeoName() string {
	return fmt.Sprintf("geo-%d", rand.Intn(900))
}

func randomXY() XY {
	return XY{
		rand.Intn(2001) - 1000,
		rand.Intn(2001) - 1000,
	}
}
