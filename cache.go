package dscache

import (
	"fmt"
	"os"
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
type MultiError map[string]error

func (m MultiError) Error() string {
	return fmt.Sprintf("MultiError: %d", m.Count())
}

// Key returns error for specified key
func (m MultiError) Key(k string) error {
	if v, ok := m[k]; ok {
		return v
	}
	return nil
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
func (c *Cache) Get(key string) ([]byte, error) {

	conn := c.Pool.Get()
	defer conn.Close()

	data, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		return nil, err
	}
	return data, nil
}

// GetMulti get multiple items from cache, returns a map of key: value in bytes.
// It will always return the same length of map from keys give if it returns a multiError
func (c *Cache) GetMulti(keys ...string) (map[string][]byte, error) {
	conn := c.Pool.Get()
	defer conn.Close()

	var (
		kArgs []interface{}
		data  [][]byte
	)
	out := make(map[string][]byte)
	mErr := make(MultiError)

	for _, key := range keys {
		kArgs = append(kArgs, key)
	}

	data, err := redis.ByteSlices(conn.Do("MGET", kArgs...))
	if err != nil {
		return nil, fmt.Errorf("dscache.Cache.GetMulti: failed %v", err)
	}

	for i, v := range data {
		k := keys[i]
		out[k] = v
		if v == nil {
			mErr[k] = redis.ErrNil
		} else {
			mErr[k] = nil
		}
	}

	if mErr.Count() > 0 {
		return out, mErr
	}

	return out, nil
}

// Set a single item in cache
func (c *Cache) Set(key string, value []byte, expire time.Duration) error {

	conn := c.Pool.Get()
	defer conn.Close()

	var err error
	if expire >= time.Second {
		_, err = conn.Do("SETEX", key, int(expire.Seconds()), value)
	} else {
		_, err = conn.Do("SET", key, value)
	}
	if err != nil {
		return err
	}

	return nil
}

// SetMulti sets multiple items in cache
func (c *Cache) SetMulti(items map[string][]byte, expire time.Duration) error {

	conn := c.Pool.Get()
	defer conn.Close()

	var (
		keys   []string
		params []interface{}
		mErr   MultiError
	)

	for key, val := range items {
		keys = append(keys, key)
		params = append(params, key)
		params = append(params, val)
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
				mErr[key] = fmt.Errorf("dscache.Cache.SetMulti: EXPIRE %s Error %v", key, err)
			} else {
				mErr[key] = nil
			}
		}
	}

	if mErr.Count() > 0 {
		return mErr
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
