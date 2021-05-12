package main

import (
	"log"
	brokerImpl "test/broker-impl"
	"test/cli"
	"test/geo"
	"time"
)

func main() {
	broker := brokerImpl.NewBrokerImpl()
	broker.Start()

	geos := make([]*geo.GeoService, 5)
	for i := 0; i < 5; i++ {
		geos[i] = geo.NewGeoService()
		ch, err := broker.Register(geos[i].GetName())
		if err != nil {
			log.Println(err)
		} else {
			geos[i].Start(broker, ch)
		}
	}

	cli := cli.NewCliService()
	ch, err := broker.Register(cli.GetName())
	if err != nil {
		log.Println(err)
	} else {
		cli.Start(broker, ch)
	}

	time.Sleep(time.Minute)

	for i := 0; i < 5; i++ {
		geos[i].Stop()
	}
}
