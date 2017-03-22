package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

const (
	RedisManagerDefaultMaxIdle     = 1
	RedisManagerDefaultMaxActive   = 10
	RedisManagerDefaultIdleTimeout = 30 * time.Second
	RedisManagerDefaultHost        = "127.0.0.1"
	RedisManagerDefaultPort        = 6379
	RedisManagerDefaultPassword    = ""
	RedisManagerDefaultDb          = 0
)

type RedisManager struct {
	maxIdle     int
	maxActive   int
	idleTimeout time.Duration
	host        string
	port        int
	password    string
	db          int
}

func NewRedisManager(host string, port int, password string, db int) *RedisManager {
	return &RedisManager{
		maxIdle: RedisManager
	}
}

func (redisMgr *RedisManager) getClient() {}

func (redisMgr *RedisManager) Set(key string, value string) error {
	return nil
}

func (redisMgr *RedisManager) Get(key string) (string, error) {
	return "", nil
}

func main() {
	redisClient = &redis.Pool{
		MaxIdle:     1,
		MaxActive:   10,
		IdleTimeout: 30 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1")
			if err != nil {
				return nil, err
			}
			c.Do("SELECT", "0")
		}
	}
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.Close()

	c.Do("MULTI")
	c.Do("SET", "test", "ming")
	v, err := c.Do("EXEC")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(v)
}
