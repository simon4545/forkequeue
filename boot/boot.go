package main

import (
	"forkequeue/levelqueue"
	"log"
	"math/rand"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	opts := levelqueue.NewOptions()
	server := levelqueue.New(opts)
	err := server.LoadMetadata()
	if err != nil {
		log.Fatalf("failed to load metadata - %s\n", err)
	}
	err = server.PersistMetadata()
	if err != nil {
		log.Fatalf("failed to persist metadata - %s\n", err)
	}
	log.Println(server.Main())
}
