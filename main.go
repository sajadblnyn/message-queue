package main

import (
	"log"

	"github.com/sajadblnyn/message-queue/server"
)

func main() {
	server := server.NewServer("127.0.0.1:8080")
	log.Fatal(server.Run())
}
