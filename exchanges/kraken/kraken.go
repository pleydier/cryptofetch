package kraken

import (
	"cryptoFetch/exchanges/ws"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"gonum.org/v1/gonum/floats"
	"log"
	"sort"
	"strconv"
)

const WsUrl = "ws.kraken.com:443"
const N = 1000

// Message parsing
type subscribeMsg struct {
	Event        string                 `json:"event"`
	Pair         []string               `json:"pair"`
	Subscription map[string]interface{} `json:"subscription"`
}

type SubscriptionReceptionMsg struct {
	channelID   int
	channelName string
	event       string
	pair        string
	status      string
}

type ObSnapshotMsg struct {
	channelId   int
	as          [][3]string
	bs          [][3]string
	channelName string
	pair        string
}

type ObUpdateMsg struct {
	channelId   int
	a           [][3]string
	b           [][3]string
	channelName string
	pair        string
}

// ListenToBestOrders subscribes and listens to N messages of the Kraken websocket API.
// After every updates, it sends the best orders through the bestOrders channel.
// The values received from bestOrders are structured in the following way:
// {{bestBidPrice, bestBidSize}, {bestAskPrice, bestAskSize}}
func ListenToBestOrders(bestOrders chan<- [2][2]float64, instrument string) {
	// Connection
	c := ws.Connect("wss", WsUrl, "/")
	_, _, err := c.ReadMessage() // There is a message received right after the connection
	if err != nil {
		log.Fatalf("Error during during Kraken websocket connection confirmation"+
			" message reception. %s", err.Error())
	}

	// Send Subscription
	Subscribe(c, []string{instrument}, map[string]interface{}{"name": "book", "depth": 25})

	// Parse the snapshot
	ob, err := HandleObSnapshotMessage(c)
	if err != nil {
		log.Fatalf("Error during during Kraken order book snapshot reception. %s", err.Error())
	}

	for i := 0; i < N; i++ {
		updated, err := HandleObUpdateMsg(c, ob)
		if err != nil {
			log.Fatalf("Error during during Kraken order book update reception. %s", err.Error())
		}
		if updated && len(ob.BidPrices) > 0 {
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
	err = c.Close()
	if err != nil {
		log.Fatalf("Could not close the connection. %s", err.Error())
	}
	fmt.Println("Connection closed.")

	fmt.Printf("Order book after %d messages contains %d bids and %d asks.\n",
		N, len(ob.BidPrices), len(ob.AskPrices))
}

// Subscribe sends a subscription message to the server
func Subscribe(c *websocket.Conn, pairs []string, subscriptions map[string]interface{}) {
	fmt.Println("Sending subscription...")
	s := subscribeMsg{
		Event:        "subscribe",
		Pair:         pairs,
		Subscription: subscriptions,
	}
	err := c.WriteJSON(s)
	if err != nil {
		log.Fatalf("Could not send the Subscription Message. %s", err.Error())
	}

	var subscriptionReceptionMsg SubscriptionReceptionMsg
	err = c.ReadJSON(&subscriptionReceptionMsg)
	if err != nil {
		log.Fatalf("Error during Subscription. %s", err.Error())
	}
}

func HandleObSnapshotMessage(c *websocket.Conn) (*ws.OrderBook, error) {
	// Read the received message
	var obSnapshotMsg ObSnapshotMsg
	_, rawMsg, err := c.ReadMessage()
	if err != nil {
		return nil, err
	}
	var tmp []interface{}
	if err := json.Unmarshal(rawMsg, &tmp); err != nil {
		return nil, err
	}
	// Parse the snapshot message
	obSnapshotMsg.channelId = int(tmp[0].(float64))
	orders := tmp[1].(map[string]interface{})
	for _, orderInterface := range orders["as"].([]interface{}) {
		orderSliceInterface := orderInterface.([]interface{})
		order := [3]string{
			fmt.Sprint(orderSliceInterface[0]),
			fmt.Sprint(orderSliceInterface[1]),
			fmt.Sprint(orderSliceInterface[2]),
		}
		obSnapshotMsg.as = append(obSnapshotMsg.as, order)
	}
	orders = tmp[1].(map[string]interface{})
	for _, orderInterface := range orders["bs"].([]interface{}) {
		orderSliceInterface := orderInterface.([]interface{})
		order := [3]string{
			fmt.Sprint(orderSliceInterface[0]),
			fmt.Sprint(orderSliceInterface[1]),
			fmt.Sprint(orderSliceInterface[2]),
		}
		obSnapshotMsg.bs = append(obSnapshotMsg.bs, order)
	}
	obSnapshotMsg.channelName = tmp[2].(string)
	obSnapshotMsg.pair = tmp[3].(string)

	// Create the local order book
	var ob ws.OrderBook
	// Bids are sorted in descending order, iterate them reversely
	for i := len(obSnapshotMsg.bs) - 1; i > 0; i-- {
		p, err := strconv.ParseFloat(obSnapshotMsg.bs[i][0], 64)
		if err != nil {
			log.Fatalf("Error parsing order book snapshot reception. %s", err.Error())
		}
		s, err := strconv.ParseFloat(obSnapshotMsg.bs[i][1], 64)
		if err != nil {
			log.Fatalf("Error parsing order book snapshot reception. %s", err.Error())
		}
		ob.BidPrices = append(ob.BidPrices, p)
		ob.BidSizes = append(ob.BidSizes, s)
	}
	for _, ask := range obSnapshotMsg.as {
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

	return &ob, nil
}

// HandleObUpdateMsg tries to parse the next message received as an order book update.
// Returns a boolean explaining whether the given order book has been updated or not
func HandleObUpdateMsg(c *websocket.Conn, ob *ws.OrderBook) (bool, error) {
	// Read the received message
	var obUpdateMsg ObUpdateMsg
	_, rawMsg, err := c.ReadMessage()
	if err != nil {
		return false, err
	}

	// If it is a heartbeat, we are able to parse it this way
	var heartbeatMsg map[string]string
	if err := json.Unmarshal(rawMsg, &heartbeatMsg); err == nil {
		return false, nil // No update of the order book
	}

	// If it is a classic update message
	var tmp []interface{}
	if err := json.Unmarshal(rawMsg, &tmp); err != nil {
		return false, err
	}
	obUpdateMsg.channelId = int(tmp[0].(float64))
	orders := tmp[1].(map[string]interface{})
	if asks, ok := orders["a"]; ok {
		for _, ask := range asks.([]interface{}) {
			foundAsk := [3]string{
				ask.([]interface{})[0].(string),
				ask.([]interface{})[1].(string),
				ask.([]interface{})[2].(string),
			}
			obUpdateMsg.a = append(obUpdateMsg.a, foundAsk)
		}
	}
	if bids, ok := orders["b"]; ok {
		for _, bid := range bids.([]interface{}) {
			foundBid := [3]string{
				bid.([]interface{})[0].(string),
				bid.([]interface{})[1].(string),
				bid.([]interface{})[2].(string),
			}
			obUpdateMsg.b = append(obUpdateMsg.b, foundBid)
		}
	}
	obUpdateMsg.channelName = tmp[2].(string)
	obUpdateMsg.pair = tmp[3].(string)

	// Apply the updates
	for _, a := range obUpdateMsg.a {
		ws.UpdateOb(a[0], a[1], &ob.AskPrices, &ob.AskSizes)
	}
	for _, b := range obUpdateMsg.b {
		ws.UpdateOb(b[0], b[1], &ob.BidPrices, &ob.BidSizes)
	}

	return true, nil
}
