package main

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
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
	pool        *redis.Pool
}

func NewRedisManager(host string, port int, password string, db int) *RedisManager {
	redisMgr := &RedisManager{
		maxIdle:     RedisManagerDefaultMaxIdle,
		maxActive:   RedisManagerDefaultMaxActive,
		idleTimeout: RedisManagerDefaultIdleTimeout,
		host:        host,
		port:        port,
		password:    password,
		db:          db,
		pool:        nil,
	}
	redisMgr.pool = redisMgr.init()
	return redisMgr
}

func NewRedisManagerWithPool(host string, port int, password string, db int, maxIdle int, maxActive int, idleTimeout time.Duration) *RedisManager {
	redisMgr := &RedisManager{
		maxIdle:     maxIdle,
		maxActive:   maxActive,
		idleTimeout: idleTimeout,
		host:        host,
		port:        port,
		password:    password,
		db:          db,
	}
	redisMgr.pool = redisMgr.init()
	return redisMgr
}

func (redisMgr *RedisManager) init() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     1,
		MaxActive:   10,
		IdleTimeout: 30 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", fmt.Sprintf("%s:%d", redisMgr.host, redisMgr.port))
			if err != nil {
				return nil, err
			}
			c.Do("SELECT", string(redisMgr.db))
			return c, nil
		},
	}
}

func (redisMgr *RedisManager) Set(key string, str string) error {
	return nil
}

func (redisMgr *RedisManager) Get(key string) (string, error) {
	return "", nil
}

func (redisMgr *RedisManager) SetObject(key string, obj interface{}) {}

func (redisMgr *RedisManager) GetObject(key string, obj interface{}) {}

func main() {
	redisMgr := NewRedisManagerWithPool("127.0.0.1", 6379, "", 0, 1, 10, 30*time.Second)
	log.Debug(redisMgr)
	c := redisMgr.pool.Get()
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
