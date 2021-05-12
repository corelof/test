package broker

import (
	"math/rand"
	"time"
)

const (
	ServiceTimeout      = time.Second * 4
	ServicePingInterval = time.Second * 2
)

type Broker interface {
	Register(string) (<-chan Message, error)
	Broadcast(Message) error
	Send(Message) (chan Message, error)
	Response(Message) error
	GetServices() []string
}

const (
	TypePing             = "ping"
	TypeDirectMessage    = "direct-send"
	TypeDirectResponse   = "direct-response"
	TypeBroadcastMessage = "broadcast"
)

type MessagePayload interface{}

type Message struct {
	MessageType string
	From        string
	To          string
	Payload     MessagePayload
	ID          uint64
}

func GenerateMessageID() uint64 {
	return rand.Uint64()
}
