package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Define a global variable to store the messages
var messages []int

func main() {
	n := maelstrom.NewNode()

	// Handle "broadcast"
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Store the message
		messages = append(messages, int(body["message"].(float64)))

		// Delete message from response
		delete(body, "message")

		// Return a broadcast_ok message
		body["type"] = "broadcast_ok"
		return n.Reply(msg, body)
	})

	// Handle "read"
	n.Handle("read", func(msg maelstrom.Message) error {
		// Send back all the messages we've received
		body := map[string]any{
			"type":     "read_ok",
			"messages": messages,
		}
		return n.Reply(msg, body)
	})

	// Handle "topology"
	n.Handle("topology", func(msg maelstrom.Message) error {
		// For now, we just acknowledge receipt of the topology
		body := map[string]any{
			"type": "topology_ok",
		}
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
