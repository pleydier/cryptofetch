package main

import (
	"cryptoFetch/exchanges/coinbase"
	"cryptoFetch/exchanges/kraken"
	"fmt"
)

func main() {
	coinbaseBestOrders := make(chan [2][2]float64, 1000)
	krakenBestOrders := make(chan [2][2]float64, 1000)

	// Goroutines listening to each exchange and returning the best bid / best ask each update
	go coinbase.ListenToBestOrders(coinbaseBestOrders, "BTC-USD")
	go kraken.ListenToBestOrders(krakenBestOrders, "XBT/USD")

	var cbseBestOrders [2][2]float64
	var krknBestOrders [2][2]float64
	for {
		select {
		case cbseBestOrders = <-coinbaseBestOrders:
		case krknBestOrders = <-krakenBestOrders:
		}
		fmt.Printf("cbse->krkn: %f\n", (cbseBestOrders[1][0]-krknBestOrders[0][0])/cbseBestOrders[1][0])
		fmt.Printf("krkn->cbse: %f\n", (krknBestOrders[1][0]-cbseBestOrders[0][0])/krknBestOrders[1][0])
	}
}
