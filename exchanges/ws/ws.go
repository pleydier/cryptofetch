package ws

import (
	"fmt"
	"github.com/gorilla/websocket"
	"log"
	"net/url"
	"sort"
	"strconv"
)

// Connect instantiates a websocket connection to the server
func Connect(scheme string, host string, path string) *websocket.Conn {
	u := url.URL{
		Scheme: scheme,
		Host:   host,
		Path:   path,
	}
	fmt.Print("Connecting to ", u.String(), "...\n")
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatalf("Could not initiate the connection. %s", err.Error())
	}

	return c
}

// OrderBook sync
type OrderBook struct {
	BidPrices []float64
	BidSizes  []float64
	AskPrices []float64
	AskSizes  []float64
}

// UpdateOb updates the order book state according to a given change.
// Can either append a new order, update or delete an existing one.
func UpdateOb(price string, size string, orderPrices *[]float64, orderSizes *[]float64) {
	p, err := strconv.ParseFloat(price, 64)
	if err != nil {
		log.Fatalf("Error parsing l2update reception. %s", err.Error())
	}
	s, err := strconv.ParseFloat(size, 64)
	if err != nil {
		log.Fatalf("Error parsing l2update reception. %s", err.Error())
	}
	idx := sort.SearchFloat64s(*orderPrices, p)
	if idx < len(*orderPrices) && (*orderPrices)[idx] == p {
		// If the price level exists, update it
		if s == 0 {
			// remove the order price level
			*orderPrices = append((*orderPrices)[:idx], (*orderPrices)[idx+1:]...)
			*orderSizes = append((*orderSizes)[:idx], (*orderSizes)[idx+1:]...)
			//fmt.Print("-")
		} else {
			// replace this price level size
			(*orderSizes)[idx] = s
			//fmt.Print("~")
		}
	} else {
		// If the price level does not exist, append it
		*orderPrices = append((*orderPrices)[:idx+1], (*orderPrices)[idx:]...)
		(*orderPrices)[idx] = p
		*orderSizes = append((*orderSizes)[:idx+1], (*orderSizes)[idx:]...)
		(*orderSizes)[idx] = s
		//fmt.Print("+")
	}
}
