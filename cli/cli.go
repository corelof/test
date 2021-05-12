package cli

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"strings"
	"test/broker"
	"test/proto"
	"time"
)

type CliService struct {
	name string

	activeWorkers int
	stop          chan chan struct{}
	broker        broker.Broker
	producer      <-chan broker.Message
}

func NewCliService() *CliService {
	return &CliService{
		randomCliName(),
		0, make(chan chan struct{}),
		nil, nil,
	}
}

func (cs *CliService) GetName() string {
	return cs.name
}

func (cs *CliService) Start(b broker.Broker, producer <-chan broker.Message) {
	cs.broker = b
	cs.producer = producer
	cs.activeWorkers = 2
	go cs.pinger()
	go cs.stdinHandler()
}

func (cs *CliService) pinger() {
	t := time.NewTicker(broker.ServicePingInterval)
	for {
		select {
		case <-t.C:
			if cs.broker != nil {
				_, err := cs.broker.Send(broker.Message{MessageType: broker.TypePing, From: cs.GetName()})
				if err != nil {
					log.Println(err)
				}
			}
		case ch := <-cs.stop:
			close(ch)
			return
		}
	}
}

func (cs *CliService) getNearestTo(x, y int) {
	unresponded := 0
	chans := make(map[string]chan broker.Message)
	for _, name := range cs.broker.GetServices() {
		if strings.HasPrefix(name, "geo-") {
			ch, err := cs.broker.Send(broker.Message{
				MessageType: broker.TypeDirectMessage,
				From:        cs.GetName(),
				To:          name,
				Payload:     proto.GetDistanceRequest{PointX: x, PointY: y},
				ID:          broker.GenerateMessageID(),
			})
			if err != nil {
				log.Println(err)
			}
			chans[name] = ch
			unresponded++
		}
	}
	nearestName := ""
	nearestDist := math.MaxFloat64
	for unresponded > 0 {
		for k, v := range chans {
			select {
			case resp := <-v:
				unresponded--
				protoResp, ok := resp.Payload.(proto.GetDistanceResponse)
				if ok && protoResp.Distance != nil {
					if nearestDist > *protoResp.Distance {
						nearestDist = *protoResp.Distance
						nearestName = k
					}
				}
			default:
				continue
			}
		}
	}
	if nearestName != "" {
		log.Printf("Nearest service is %s, distance is %v", nearestName, nearestDist)
	} else {
		log.Printf("No geo services found")
	}
}

func (cs *CliService) stdinHandler() {
	for {
		var smb string
		fmt.Scanln(&smb)
		if smb == "1" {
			var x, y int
			fmt.Scan(&x, &y)
			cs.getNearestTo(x, y)
		} else if smb == "2" {
			err := cs.broker.Broadcast(broker.Message{
				MessageType: broker.TypeBroadcastMessage,
				From:        cs.GetName(),
				Payload:     proto.UpdateLocationRequest{},
			})
			if err != nil {
				log.Println(err)
			}
		} else {
			log.Println("got wrong command")
		}
		select {
		case ch := <-cs.stop:
			close(ch)
			return
		default:
			continue
		}
	}
}

func (cs *CliService) Stop() {
	for i := 0; i < cs.activeWorkers; i++ {
		ch := make(chan struct{})
		cs.stop <- ch
		<-ch
	}
}

func randomCliName() string {
	return fmt.Sprintf("cli-%d", rand.Intn(900))
}
