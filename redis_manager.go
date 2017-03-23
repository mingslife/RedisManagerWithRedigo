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

func (redisMgr *RedisManager) getTempKey(key string) string {
	return "tmp/" + key
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

func (redisMgr *RedisManager) CheckObject(key string) error {
	c := redisMgr.getConnection()
	defer c.Close()

	statusKey := redisMgr.getStatusKey(key)

	c.Do("MULTI")
	c.Do("SET", statusKey, RedisManagerStatusChecked)
	_, err := c.Do("EXEC")

	return err
}

func (redisMgr *RedisManager) getStudentKey(key string, id int) string {
	return fmt.Sprintf("%s/%d", key, id)
}

func (redisMgr *RedisManager) SetStudents(key string, students []*Student) error {
	c := redisMgr.getConnection()
	defer c.Close()

	c.Do("MULTI")
	for _, student := range students {
		studentId := student.Id
		// log.Info(student)
		studentKey := redisMgr.getStudentKey(key, studentId)
		bytes, err := json.Marshal(student)
		if err != nil {
			log.Error(err.Error())
			c.Do("DISCARD")
			return err
		}
		c.Do("SET", studentKey, bytes)
		c.Do("HMSET", key, studentId, RedisManagerStatusUncheck)
	}
	_, err := c.Do("EXEC")

	return err
}

func (redisMgr *RedisManager) GetStudents(key string) ([]*Student, error) {
	c := redisMgr.getConnection()
	defer c.Close()

	studentIds, err := redis.Ints(c.Do("HKEYS", key))
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	c.Do("MULTI")
	for _, studentId := range studentIds {
		studentKey := redisMgr.getStudentKey(key, studentId)
		c.Do("GET", studentKey)
		// log.Info(studentKey)
	}
	values, err := redis.ByteSlices(c.Do("EXEC"))
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	students := make([]*Student, 0, len(values))
	for _, value := range values {
		student := &Student{}
		// log.Info(value)
		// log.Info(string(value))
		err = json.Unmarshal(value, student)
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}
		students = append(students, student)
	}

	return students, err
}

func (redisMgr *RedisManager) GetStudent(key string, id int) (*Student, error) {
	c := redisMgr.getConnection()
	defer c.Close()

	studentKey := redisMgr.getStudentKey(key, id)
	value, err := redis.Bytes(c.Do("GET", studentKey))
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	student := &Student{}
	err = json.Unmarshal(value, student)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return student, nil
}

func (redisMgr *RedisManager) GetStudentStatus(key string, id int) (int, error) {
	c := redisMgr.getConnection()
	defer c.Close()

	status, err := redis.Int(c.Do("HGET", key, id))
	if err != nil {
		log.Error(err.Error())
		status = RedisManagerStatusError
	}

	return status, err
}

func (redisMgr *RedisManager) DelStudents(key string) error {
	c := redisMgr.getConnection()
	defer c.Close()

	studentIds, err := redis.Ints(c.Do("HKEYS", key))
	if err != nil {
		log.Error(err.Error())
		return err
	}
	c.Do("MULTI")
	for _, studentId := range studentIds {
		studentKey := redisMgr.getStudentKey(key, studentId)
		c.Do("DEL", studentKey)
	}
	c.Do("DEL", key)
	_, err = c.Do("EXEC")
	if err != nil {
		log.Error(err.Error())
	}

	return err
}

func (redisMgr *RedisManager) DelStudent(key string, id int) error {
	c := redisMgr.getConnection()
	defer c.Close()

	studentKey := redisMgr.getStudentKey(key, id)

	c.Do("MULTI")
	c.Do("DEL", studentKey)
	c.Do("HDEL", key, id)
	_, err := c.Do("EXEC")
	if err != nil {
		log.Error(err.Error())
	}

	return err
}

func (redisMgr *RedisManager) CheckStudent(key string, id int) error {
	c := redisMgr.getConnection()
	defer c.Close()

	tempKey := redisMgr.getTempKey(key)
	studentKey := redisMgr.getStudentKey(key, id)
	studentTempKey := redisMgr.getTempKey(studentKey)

	c.Do("MULTI")
	c.Do("RENAME", studentKey, studentTempKey)
	c.Do("SADD", tempKey, id)
	// c.Do("EXPIRE", studentTempKey, redisMgr.expireTime)
	// c.Do("EXPIRE", tempKey, redisMgr.expireTime)
	c.Do("EXPIRE", studentTempKey, 60)
	c.Do("EXPIRE", tempKey, 60)
	c.Do("HDEL", key, id)
	_, err := c.Do("EXEC")
	if err != nil {
		log.Error(err.Error())
	}

	return err
}

type Student struct {
	Id   int
	Name string
}

func main() {
	student1 := &Student{
		Id:   1,
		Name: "Ming",
	}
	student2 := &Student{
		Id:   2,
		Name: "huangzeming",
	}
	student3 := &Student{
		Id:   3,
		Name: "zeming",
	}
	students := make([]*Student, 0)
	students = append(students, student1)
	students = append(students, student2)
	students = append(students, student3)

	redisMgr := NewRedisManagerWithPool("127.0.0.1", 6379, "", 0, 1, 10, 30*time.Second)
	redisMgr.SetStudents("students/cqut", students)

	redisMgr.DelStudent("students/cqut", 2)

	queryStudents, _ := redisMgr.GetStudents("students/cqut")
	log.Info(queryStudents)
	for _, queryStudent := range queryStudents {
		log.Info(queryStudent)
	}
	redisMgr.CheckStudent("students/cqut", 3)

	log.Info(redisMgr.GetStudentStatus("students/cqut", 1))
	log.Info(redisMgr.GetStudentStatus("students/cqut", 2))
	log.Info(redisMgr.GetStudentStatus("students/cqut", 3))

	redisMgr.DelStudents("students/cqut")
}

func main0() {
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
