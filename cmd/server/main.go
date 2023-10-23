package main

import (
	server2 "github.com/a-shakra/commit-log/internal/server"
	"log"
)

func main() {
	server := server2.NewHTTPServer(":8080")
	log.Fatal(server.ListenAndServe())
}
