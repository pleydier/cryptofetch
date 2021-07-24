package coinbase

import (
	"cryptoFetch/exchanges/ws"
	"fmt"
	"github.com/gorilla/websocket"
	"gonum.org/v1/gonum/floats"
	"log"
	"sort"
	"strconv"
	"time"
)

const WsUrl = "ws-feed.pro.coinbase.com:443"
const N = 1000

// Message parsing
type subscribeMsg struct {
	Type       string   `json:"type"`
	ProductIds []string `json:"product_ids"`
	Channels   []string `json:"channels"`
}

type obMsg struct {
	Type      string      `json:"type"`
	ProductId string      `json:"product_id"`
	Bids      [][2]string `json:"bids"`
	Asks      [][2]string `json:"asks"`
	Time      time.Time   `json:"time"`
	Changes   [][3]string `json:"changes"`
}

// ListenToBestOrders subscribes and listens to N messages of the Coinbase websocket API.
// After every updates, it sends the best orders through the bestOrders channel.
// The values received from bestOrders are structured in the following way:
// {{bestBidPrice, bestBidSize}, {bestAskPrice, bestAskSize}}
func ListenToBestOrders(bestOrders chan<- [2][2]float64, instrument string) {
	var ob ws.OrderBook
	// Connection
	c := ws.Connect("wss", WsUrl, "/")

	// Send subscription
	Subscribe(c, []string{instrument}, []string{"level2", "heartbeat"})

	var msg obMsg
	for i := 0; i < N; i++ {
		err := c.ReadJSON(&msg)
		if err != nil {
			log.Fatalf("Error during Message reception. %s", err.Error())
		}
		HandleMessage(msg, &ob)
		if len(ob.BidPrices) > 0 {
			bestBidIdx := floats.MaxIdx(ob.BidPrices)
			bestAskIdx := floats.MinIdx(ob.AskPrices)
			bestOrders <- [2][2]float64{
				{ob.BidPrices[bestBidIdx], ob.BidSizes[bestBidIdx]},
				{ob.AskPrices[bestAskIdx], ob.AskSizes[bestAskIdx]},
			}
		}
	}
	close(bestOrders)

	fmt.Println("\nClosing the connection...")
	err := c.Close()
	if err != nil {
		log.Fatalf("Could not close the connection. %s", err.Error())
	}
	fmt.Println("Connection closed.")

	fmt.Printf("Order book after %d messages contains %d bids and %d asks.\n",
		N, len(ob.BidPrices), len(ob.AskPrices))
}

// Subscribe sends a subscribtion message to the server
func Subscribe(c *websocket.Conn, productIds []string, channels []string) {
	fmt.Println("Sending subscribtion...")
	subscription := subscribeMsg{
		Type:       "subscribe",
		ProductIds: productIds,
		Channels:   channels,
	}
	err := c.WriteJSON(subscription)
	if err != nil {
		log.Fatalf("Could not send the subscription Message. %s", err.Error())
	}
}

// HandleMessage reads a JSON server message, parses it, and updates the
// order book accordingly
func HandleMessage(msg obMsg, ob *ws.OrderBook) {
	switch msg.Type {
	case "subscriptions": // Subscription response
		fmt.Println("Subscription successful!")
	case "snapshot": // Initial order book snapshot
		HandleObSnapshotMsg(msg, ob)
	case "l2update": // Order book update
		HandleL2UpdateMsg(msg, ob)
	case "heartbeat":
		fmt.Println("\nPing time: ", time.Now().Sub(msg.Time))
	}
}

// HandleObSnapshotMsg uses an order book snapshot message to update the local order
// book accordingly
func HandleObSnapshotMsg(msg obMsg, ob *ws.OrderBook) {
	// Bids are sorted in descending order, iterate them reversely
	for i := len(msg.Bids) - 1; i > 0; i-- {
		p, err := strconv.ParseFloat(msg.Bids[i][0], 64)
		if err != nil {
			log.Fatalf("Error parsing order book snapshot reception. %s", err.Error())
		}
		s, err := strconv.ParseFloat(msg.Bids[i][1], 64)
		if err != nil {
			log.Fatalf("Error parsing order book snapshot reception. %s", err.Error())
		}
		ob.BidPrices = append(ob.BidPrices, p)
		ob.BidSizes = append(ob.BidSizes, s)
	}
	for _, ask := range msg.Asks {
		p, err := strconv.ParseFloat(ask[0], 64)
		if err != nil {
			log.Fatalf("Error parsing order book snapshot reception. %s", err.Error())
		}
		s, err := strconv.ParseFloat(ask[1], 64)
		if err != nil {
			log.Fatalf("Error parsing order book snapshot reception. %s", err.Error())
		}
		ob.AskPrices = append(ob.AskPrices, p)
		ob.AskSizes = append(ob.AskSizes, s)
	}

	// Check the integrity of the book
	if !sort.Float64sAreSorted(ob.BidPrices) {
		fmt.Println("Bid prices not sorted.")
	}
	if !sort.Float64sAreSorted(ob.AskPrices) {
		fmt.Println("Ask prices not sorted.")
	}
	fmt.Printf("Initial order book snapshot contains %d bids and %d asks.\n",
		len(ob.BidPrices), len(ob.AskPrices))
}

// HandleL2UpdateMsg uses a level2 update message to update the local order book accordingly.
func HandleL2UpdateMsg(msg obMsg, ob *ws.OrderBook) {
	for _, change := range msg.Changes {
		switch change[0] {
		case "buy":
			ws.UpdateOb(change[1], change[2], &ob.BidPrices, &ob.BidSizes)
		case "sell":
			ws.UpdateOb(change[1], change[2], &ob.AskPrices, &ob.AskSizes)
		}
	}
}
