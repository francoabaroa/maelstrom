package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type NodeState struct {
	node     *maelstrom.Node
	id       string
	messages []int
	peers    []string
	mu       sync.Mutex
}

type BroadcastRequestBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type BroadcastResponseBody struct {
	Type string `json:"type"`
}

type BroadcastOkResponseBody struct {
	InReplyTo int    `json:"in_reply_to"`
	Type      string `json:"type"`
}

func containsInt(slice []int, item int) bool {
	for _, a := range slice {
		if a == item {
			return true
		}
	}
	return false
}

func containsStr(slice []string, item string) bool {
	for _, a := range slice {
		if a == item {
			return true
		}
	}
	return false
}

func main() {
	n := maelstrom.NewNode()
	state := &NodeState{node: n}

	// Handle "init"
	n.Handle("init", func(msg maelstrom.Message) error {
		var body struct {
			Type    string   `json:"type"`
			MsgID   int      `json:"msg_id"`
			NodeID  string   `json:"node_id"`
			NodeIDs []string `json:"node_ids"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.mu.Lock()
		state.id = body.NodeID
		state.mu.Unlock()

		reply := map[string]interface{}{
			"type":        "init_ok",
			"in_reply_to": body.MsgID,
		}
		return n.Reply(msg, reply)
	})

	// Handle "broadcast"
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastRequestBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.mu.Lock()
		defer state.mu.Unlock()

		// Check if we have already seen this message
		if containsInt(state.messages, body.Message) {
			reply := BroadcastResponseBody{
				Type: "broadcast_ok",
			}
			return n.Reply(msg, reply)
		}

		// Store the new message
		state.messages = append(state.messages, body.Message)

		go func(destinations []string, body BroadcastRequestBody) {
			var successfulSentDestinations []string
			var successfulSentDestinationsMutex sync.Mutex

			for len(destinations) != len(successfulSentDestinations) {
				for _, node := range destinations {
					if !containsStr(successfulSentDestinations, node) {
						n.RPC(node, body, func(msg maelstrom.Message) error {
							var broadcastOkBody BroadcastOkResponseBody

							if err := json.Unmarshal(msg.Body, &broadcastOkBody); err != nil {
								return err
							}

							if broadcastOkBody.Type != "broadcast_ok" {
								return fmt.Errorf("expected type broadcast_ok, got %s", broadcastOkBody.Type)
							}

							successfulSentDestinationsMutex.Lock()
							successfulSentDestinations = append(successfulSentDestinations, node)
							successfulSentDestinationsMutex.Unlock()

							return nil
						})
					}
				}
				time.Sleep(1 * time.Second)
			}
		}(state.peers, body)

		reply := BroadcastResponseBody{
			Type: "broadcast_ok",
		}
		return n.Reply(msg, reply)
	})

	// Handle "read"
	n.Handle("read", func(msg maelstrom.Message) error {
		var body struct {
			Type  string `json:"type"`
			MsgID int    `json:"msg_id"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.mu.Lock()
		defer state.mu.Unlock()

		reply := map[string]interface{}{
			"type":        "read_ok",
			"messages":    state.messages,
			"in_reply_to": body.MsgID,
		}
		return n.Reply(msg, reply)
	})

	// Handle "topology"
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body struct {
			Type     string              `json:"type"`
			Topology map[string][]string `json:"topology"`
			MsgID    int                 `json:"msg_id"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.mu.Lock()
		defer state.mu.Unlock()

		state.peers = body.Topology[state.id]

		reply := map[string]interface{}{
			"type":        "topology_ok",
			"in_reply_to": body.MsgID,
		}
		return n.Reply(msg, reply)
	})

	log.Fatal(n.Run())
}
