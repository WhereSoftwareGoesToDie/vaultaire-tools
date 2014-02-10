/*
Connects to an ingestd daemon on localhost, and outputs the number of
writes in a batch and the time to write the batch in seconds (in
space-separated format). 
*/
package main

import (
	zmq "github.com/pebbe/zmq4"
	"log"
	"os"
	"fmt"
	"strings"
)

func printError(msg string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, msg, v...)
}

func main() {
	sock, err := zmq.NewSocket(zmq.SUB)
	if err != nil {
		log.Fatal(err)
	}
	sock.SetSubscribe("")
	sock.Connect("tcp://localhost:5570")
	var lastWriteCount string
	for true {
		msg, err := sock.RecvMessage(0)
		if err != nil {
			printError("%v", err)
		}
		if len(msg) < 2 {
			printError("Error: need two values in message %v", msg)
			continue
		}
		key := strings.Trim(msg[0], " \t")
		value := strings.Trim(msg[1], " \t")
		if key == "writing" {
			lastWriteCount = value
		} else if key == "delta" && lastWriteCount != "" {
			fmt.Printf("%s %s\n", lastWriteCount, value)
		}
	}
}
