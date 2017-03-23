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
	RedisManagerDefaultHost        = "127.0.0.1" // No use yet
	RedisManagerDefaultPort        = 6379        // No use yet
	RedisManagerDefaultPassword    = ""          // No use yet
	RedisManagerDefaultDb          = 0           // No use yet
	RedisManagerDefaultExpireTime  = 21600       // 6 hours
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
	expireTime  int64
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
		expireTime:  RedisManagerDefaultExpireTime,
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

func (redisMgr *RedisManager) getConnection() redis.Conn {
	return redisMgr.pool.Get()
}

func (redisMgr *RedisManager) Set(key string, str string) error {
	c := redisMgr.getConnection()
	defer c.Close()

	_, err := c.Do("SET", key, str)
	if err != nil {
		log.Error(err.Error())
		return err
	}
	return nil
}

func (redisMgr *RedisManager) Get(key string) (string, error) {
	c := redisMgr.getConnection()
	defer c.Close()

	v, err := redis.String(c.Do("GET", key))
	if err != nil {
		log.Error(err.Error())
		return "", err
	}
	return v, nil
}

func (redisMgr *RedisManager) Del(key string) error {
	c := redisMgr.getConnection()
	defer c.Close()

	_, err := c.Do("DEL", key)
	if err != nil {
		log.Error(err.Error())
		return err
	}
	return nil
}

func (redisMgr *RedisManager) SetObject(key string, obj interface{}) {}

func (redisMgr *RedisManager) GetObject(key string, obj interface{}) {}

func main() {
	redisMgr := NewRedisManagerWithPool("127.0.0.1", 6379, "", 0, 1, 10, 30*time.Second)
	redisMgr.Set("test", "huangzeming")
	v, _ := redisMgr.Get("test")
	log.Info(v)
	redisMgr.Del("test")
}
