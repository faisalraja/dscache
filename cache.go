package dscache

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	cacheVersionPrefix = "dsc1:"
)

// Cache stores redis pool
type Cache struct {
	// Pool holds redis connection pool
	Pool *redis.Pool
}

// MultiError is returned for batch actions
type MultiError []error

func (m MultiError) Error() string {
	return fmt.Sprintf("MultiError: %d", m.Count())
}

// Count returns number of errors
func (m MultiError) Count() int {
	c := 0
	for _, v := range m {
		if v != nil {
			c++
		}
	}
	return c
}

// NewCache creates a cache for dscache
func NewCache(server string) *Cache {
	if server == "" {
		server = os.Getenv("REDIS_HOST")
		if server == "" {
			server = ":6379"
		}
	}
	return &Cache{
		Pool: &redis.Pool{

			MaxIdle:     3,
			IdleTimeout: 240 * time.Second,

			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", server)
				if err != nil {
					return nil, err
				}
				return c, err
			},

			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
	}
}

// Close the redis pool
func (c *Cache) Close() {
	c.Pool.Close()
}

// Ping checks if a connection can be made
func (c *Cache) Ping() error {
	conn := c.Pool.Get()
	defer conn.Close()

	_, err := redis.String(conn.Do("PING"))
	if err != nil {
		return fmt.Errorf("dscache.Cache.Ping: cannot 'PING' db: %v", err)
	}
	return nil
}

// Get an item from cache
func (c *Cache) Get(key string, value interface{}) error {

	conn := c.Pool.Get()
	defer conn.Close()

	var data []byte
	data, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(data)).Decode(value)
}

// GetMulti get multiple items from cache
func (c *Cache) GetMulti(keys []string, values interface{}) error {

	conn := c.Pool.Get()
	defer conn.Close()
	var (
		kArgs []interface{}
		out   []interface{}
		data  [][]byte
		errs  MultiError
	)
	if reflect.TypeOf(values).Kind() != reflect.Slice {
		return fmt.Errorf("dscache.Cache.GetMulti: values must be a slice")
	}
	vals := reflect.ValueOf(values)
	if len(keys) != vals.Len() {
		return fmt.Errorf("dscache.Cache.GetMulti: length of keys must be equal to length of values")
	}
	for k, key := range keys {
		kArgs = append(kArgs, key)
		out = append(out, vals.Index(k).Interface())
	}

	data, err := redis.ByteSlices(conn.Do("MGET", kArgs...))
	if err != nil {
		return err
	}

	for key, val := range data {
		var err error
		if val != nil {
			elem := reflect.ValueOf(out[key])
			if elem.Kind() != reflect.Ptr {
				err = fmt.Errorf("dscache.Cache.GetMulti: value of slice must be a pointer")
			} else {
				err = gob.NewDecoder(bytes.NewBuffer(val)).Decode(out[key])
			}
		} else {
			err = redis.ErrNil
		}

		if err != nil {
			errs = append(errs, err)
		} else {
			errs = append(errs, nil)
		}
	}
	if errs.Count() > 0 {
		return errs
	}
	return nil
}

// Set a single item in cache
func (c *Cache) Set(key string, value interface{}, expire time.Duration) error {

	conn := c.Pool.Get()
	defer conn.Close()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}

	var err error
	if expire >= time.Second {
		_, err = conn.Do("SETEX", key, int(expire.Seconds()), buf.Bytes())
	} else {
		_, err = conn.Do("SET", key, buf.Bytes())
	}
	if err != nil {
		return err
	}

	return nil
}

// SetMulti sets multiple items in cache
func (c *Cache) SetMulti(items map[string]interface{}, expire time.Duration) error {

	conn := c.Pool.Get()
	defer conn.Close()

	var (
		keys   []string
		params []interface{}
		errs   MultiError
	)

	for key, item := range items {
		keys = append(keys, key)
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(item); err != nil {
			return err
		}
		params = append(params, key)
		params = append(params, buf.Bytes())
	}

	conn.Send("MSET", params...)
	if expire >= time.Second {
		for _, key := range keys {
			conn.Send("EXPIRE", key, int(expire.Seconds()))
		}
	}
	conn.Flush()
	if _, err := conn.Receive(); err != nil {
		return err
	}
	if expire >= time.Second {
		for _, key := range keys {
			if _, err := conn.Receive(); err != nil {
				errs = append(errs, fmt.Errorf("dscache.Cache.SetMulti: EXPIRE %s Error %v", key, err))
			} else {
				errs = append(errs, nil)
			}
		}
	}

	if errs.Count() > 0 {
		return errs
	}

	return nil
}

// Exists returns if key exists in cache
func (c *Cache) Exists(key string) (bool, error) {

	conn := c.Pool.Get()
	defer conn.Close()

	ok, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return ok, fmt.Errorf("dscache.Cache.Exists: error checking if key %s exists: %v", key, err)
	}
	return ok, err
}

// Delete removes cache
func (c *Cache) Delete(key ...string) error {

	conn := c.Pool.Get()
	defer conn.Close()

	var keys []interface{}
	for _, v := range key {
		keys = append(keys, v)
	}

	_, err := conn.Do("DEL", keys...)
	return err
}

// GetKeys returns keys from pattern
func (c *Cache) GetKeys(pattern string) ([]string, error) {

	conn := c.Pool.Get()
	defer conn.Close()

	iter := 0
	keys := []string{}
	for {
		arr, err := redis.Values(conn.Do("SCAN", iter, "MATCH", pattern))
		if err != nil {
			return keys, fmt.Errorf("dscache.Cache.GetKeys: error retrieving '%s' keys", pattern)
		}

		iter, _ = redis.Int(arr[0], nil)
		k, _ := redis.Strings(arr[1], nil)
		keys = append(keys, k...)

		if iter == 0 {
			break
		}
	}

	return keys, nil
}

// Flush removes everything from cache
func (c *Cache) Flush() error {

	conn := c.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("FLUSHALL")
	return err
}
