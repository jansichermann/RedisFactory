package redisfactory

import (
	"fmt"
	"time"
	"sync"
	"github.com/fzzy/radix/redis"
	"github.com/jansichermann/redisinterface"
)

var connectionPoolCount int = 64

var connectionsAllocated *int = new(int)
var connections [100]*redis.Client
var connectionsLock sync.Mutex
var redisLock sync.Mutex

func AddConnectionToPool(client *redis.Client) {
	var i int
	var c *redis.Client
	poolSpotFound := false

	connectionsLock.Lock()

	for i, c = range(connections) {
		if c == nil {
			poolSpotFound = true
			break
		}
	}
	
	if poolSpotFound {
		connections[i] = client
	} else {
		fmt.Print("Closing This connection, Pool full\n")
		client.Close()
	}
	connectionsLock.Unlock()
}

func GetConnectionFromPool() *redis.Client {
	connectionsLock.Lock()
	var c *redis.Client
	var i int
	for i, c = range(connections) {
		if c != nil {
			connections[i] = nil
			break
		}
	}
	connectionsLock.Unlock()
	return c
}

func NewConnection() (*redis.Client, chan bool) {
	var r *redis.Client

	// we try to get a connection from the pool a number of times
	for retries := 0; retries < 20; retries ++ {
		r = GetConnectionFromPool()
		if r != nil {
			break
		}

		redisLock.Lock()
		breakOnCount := *connectionsAllocated < (connectionPoolCount / 4)
		redisLock.Unlock()
		if breakOnCount {
			break
		}

		time.Sleep(time.Millisecond * 1)
	}

	if r == nil {
		redisLock.Lock()
		*connectionsAllocated++
		r = redisinterface.SetupRedisConnection()
		redisLock.Unlock()
		fmt.Print("Connections initialized: ", *connectionsAllocated, "\n")
	}
	
	// buffered channel, as the connection 
	// may time out
	returnChannel := make(chan bool, 1)
	defer func(rChan chan bool) {
		go func() {
			select {
				case _ = <- rChan:
					AddConnectionToPool(r)
				case <- time.After(time.Second * 60):
					fmt.Print("Connection Timeout")
					r.Close()
				}
			}()
	}(returnChannel)

	return r, returnChannel
}