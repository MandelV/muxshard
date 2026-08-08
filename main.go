package main

import (
	"flag"
	"log"
)

func main() {
	mode := flag.String("mode", "", "server or client")
	addr := flag.String("addr", "127.0.0.1:9000", "TCP address")
	partitions := flag.Int("partitions", 16, "number of partitions (TCP connections)")
	flag.Parse()

	var err error
	switch *mode {
	case "server":
		err = runServer(*addr)
	case "client":
		err = runClient(*addr, *partitions)
	default:
		log.Fatal("usage: -mode=server|client [-addr=host:port] [-partitions=16]")
	}

	if err != nil {
		log.Fatal(err)
	}
}
