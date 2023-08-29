package main

import (
	"encoding/json"
	"log"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// counter for unique id generation
var counter int

// lock to prevent race condition in counter increment
var mutex sync.Mutex

func main() {
	n := maelstrom.NewNode()
	log.Println("Registering handler for generate")
	n.Handle("generate", func(msg maelstrom.Message) error {
		log.Println("Inside generate handler")
		// Unmarshal the message body into a map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// safely increment the counter
		mutex.Lock()
		id := counter
		counter++
		mutex.Unlock()

		// create a globally unique id by appending the node id to the counter
		idStr := n.ID() + "-" + strconv.Itoa(id)

		body["type"] = "generate_ok"
		body["id"] = idStr

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
