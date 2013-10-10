package redisfactory

import (
	"fmt"
	"time"
	"sync"
	"github.com/fzzy/radix/redis"
	"github.com/jansichermann/redisinterface"
)

var connections [10]*redis.Client
var connectionsLock sync.Mutex

func AddConnectionToPool(client *redis.Client) {
	connectionsLock.Lock()
	var i int
	var c *redis.Client
	poolSpotFound := false

	for i, c = range(connections) {
		if c == nil {
			poolSpotFound = true
			break
		}
	}
	
	if poolSpotFound {
		fmt.Print("Adding connection to pool at index: ", i, "\n")
		connections[i] = client
	} else {
		fmt.Print("Closing This connection, Pool full\n")
		client.Close()
	}

	connectionsLock.Unlock()
}

func GetConnectionFromPool() *redis.Client {
	connectionsLock.Lock()
	var i int
	var c *redis.Client
	for i, c = range(connections) {
		if c != nil {
			connections[i] = nil
			fmt.Print("Found Connection in Pool at index: ", i, "\n")
			break
		}
	}
	
	connectionsLock.Unlock()
	return c
}

func NewConnection() (*redis.Client, chan bool) {
	r := GetConnectionFromPool()
	if r == nil {
		fmt.Print("Creating new connection\n")
		r = redisinterface.SetupRedisConnection()
	}
	
	// buffered channel, as the connection 
	// may time out
	returnChannel := make(chan bool, 1)
	defer func(rChan chan bool) {
		go func() {
			select {
				case _ = <- rChan:
					AddConnectionToPool(r)
				case <- time.After(time.Second * 10):
					fmt.Print("Connection Timeout")
					r.Close()
				}
			}()
	}(returnChannel)

	return r, returnChannel
}