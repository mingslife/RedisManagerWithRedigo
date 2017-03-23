package main

import (
	"encoding/json"
	"errors"
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

const (
	RedisManagerStatusUncheck = iota
	RedisManagerStatusChecked
	RedisManagerStatusDirty
	RedisManagerStatusError
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

func (redisMgr *RedisManager) getStatusKey(key string) string {
	return key + "/status"
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
	}
	return err
}

func (redisMgr *RedisManager) SetObject(key string, obj interface{}) error {
	c := redisMgr.getConnection()
	defer c.Close()

	bytes, e := json.Marshal(obj)
	if e != nil {
		log.Error(e.Error())
		return e
	}
	statusKey := redisMgr.getStatusKey(key)
	status := RedisManagerStatusUncheck

	ok, err := redis.Bool(c.Do("EXISTS", statusKey))
	if err != nil {
		log.Error(err.Error())
		return err
	}
	if ok {
		v, err := redis.Int(c.Do("GET", statusKey))
		if err != nil {
			log.Error(err.Error())
			return err
		}
		if v != RedisManagerStatusChecked {
			status = RedisManagerStatusDirty
		}
	}
	c.Do("MULTI")
	c.Do("SET", key, bytes)
	c.Do("SET", statusKey, status)
	_, err = c.Do("EXEC")
	if err != nil {
		log.Error(err.Error())
	}
	return err
}

func (redisMgr *RedisManager) GetObject(key string, obj interface{}) (int, error) {
	c := redisMgr.getConnection()
	defer c.Close()

	statusKey := redisMgr.getStatusKey(key)

	status := RedisManagerStatusError
	ok, err := redis.Bool(c.Do("EXISTS", statusKey))
	if ok {
		status, err = redis.Int(c.Do("GET", statusKey))
		if err != nil {
			log.Error(err.Error())
			obj = nil
			return RedisManagerStatusError, err
		}
		bytes, err := redis.Bytes(c.Do("GET", key))
		if err != nil {
			log.Error(err.Error())
			obj = nil
			return RedisManagerStatusError, err
		}
		err = json.Unmarshal(bytes, obj)
		if err != nil {
			log.Error(err.Error())
			obj = nil
			return RedisManagerStatusError, err
		}
	} else {
		err = errors.New("RedisManager: has not status")
		log.Error(err.Error())
		obj = nil
	}
	return status, err
}

func (redisMgr *RedisManager) DelObject(key string) error {
	c := redisMgr.getConnection()
	defer c.Close()

	statusKey := redisMgr.getStatusKey(key)

	c.Do("MULTI")
	c.Do("DEL", key)
	c.Do("DEL", statusKey)
	_, err := c.Do("EXEC")
	return err
}

// TODO
func (redisMgr *RedisManager) CheckObject(key string) error {
	c := redisMgr.getConnection()
	defer c.Close()

	return nil
}

type Student struct {
	Id   int
	Name string
}

func main() {
	redisMgr := NewRedisManagerWithPool("127.0.0.1", 6379, "", 0, 1, 10, 30*time.Second)
	redisMgr.Set("test", "huangzeming")
	v, _ := redisMgr.Get("test")
	log.Info(v)
	redisMgr.Del("test")
	student := &Student{
		Id:   1,
		Name: "Ming",
	}
	redisMgr.SetObject("student/1", student)
	obj := &Student{}
	status, _ := redisMgr.GetObject("student/1", obj)
	log.Info(obj)
	log.Info(status)
	redisMgr.DelObject("student/1")
}
