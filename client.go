package dscache

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
)

// Client can replace the regular go cloud datastore client on usage.
// Instanciate this by providing the datastore client and cache information.
type Client struct {
	Context  context.Context
	DSClient *datastore.Client
	Cache    *Cache
	Expire   time.Duration

	localCache     map[string]interface{}
	localCacheLock sync.RWMutex
}

// NewClient creates a dscache Client. This should be created for every request while
// you can have a global datastore Client and cache pool.
func NewClient(ctx context.Context, dsClient *datastore.Client, cache *Cache) (*Client, error) {
	c := &Client{
		Context:    ctx,
		DSClient:   dsClient,
		Cache:      cache,
		Expire:     time.Hour * 24,
		localCache: make(map[string]interface{}),
	}
	return c, nil
}

func (c *Client) putLocalCache(key *datastore.Key, src interface{}) {
	c.localCacheLock.Lock()
	defer c.localCacheLock.Unlock()
	c.localCache[cacheKey(key)] = src
}

func (c *Client) putCache(key *datastore.Key, src interface{}) error {
	errc := make(chan error)
	go func() {
		errc <- c.Cache.Set(cacheKey(key), src, c.Expire)
	}()
	c.putLocalCache(key, src)
	if err := <-errc; err != nil {
		// we ignore put cache failure
		return fmt.Errorf("dscache.Client.putCache: failed to cache %v", err)
	}
	return nil
}

// todo transaction

// Run runs the given query in the given context.
func (c *Client) Run(ctx context.Context, q *datastore.Query) *datastore.Iterator {
	return c.DSClient.Run(ctx, q)
}

// Put saves entity into datastore, cache, context cache
func (c *Client) Put(ctx context.Context, key *datastore.Key, src interface{}) (*datastore.Key, error) {
	var err error
	key, err = c.DSClient.Put(ctx, key, src)
	if err != nil {
		return nil, fmt.Errorf("dscache.Client.Put: failed error %v", err)
	}

	if err := c.putCache(key, src); err != nil {
		// todo make sure it's a success
		log.Printf(err.Error())
	}

	return key, nil
}

// PutMulti is the batch version of Put
func (c *Client) PutMulti(ctx context.Context, keys []*datastore.Key, src interface{}) ([]*datastore.Key, error) {
	keys, err := c.DSClient.PutMulti(ctx, keys, src)
	if err != nil {
		return nil, fmt.Errorf("dscache.Client.PutMulti: failed %v", err)
	}
	vals := reflect.ValueOf(src)
	valMap := make(map[string]interface{})
	for idx, key := range keys {
		val := vals.Index(idx).Interface()
		c.putLocalCache(key, val)
		valMap[cacheKey(key)] = val
	}
	if err := c.Cache.SetMulti(valMap, c.Expire); err != nil {
		// todo make sure it's a success
		log.Printf("dscache.Client.PutMulti: failed to cache %v", err)
	}
	return keys, nil
}

// Get checks local cache, then memcache, then datastore. Caches it if it's not in cache yet.
func (c *Client) Get(ctx context.Context, key *datastore.Key, dst interface{}) error {
	set := reflect.ValueOf(dst)
	if set.Kind() != reflect.Ptr {
		return fmt.Errorf("dscache.Client.Get: dst must be a pointer")
	}
	if !set.CanSet() {
		set = set.Elem()
	}
	found := false
	cKey := cacheKey(key)

	// Check local cache first
	c.localCacheLock.RLock()
	if val, ok := c.localCache[cKey]; ok {
		set.Set(reflect.Indirect(reflect.ValueOf(val)))
		found = true
	}
	c.localCacheLock.RUnlock()

	// Check redis cache
	if !found {
		if err := c.Cache.Get(cKey, dst); err == nil {
			c.putLocalCache(key, dst)
			found = true
		}
	}

	// get from datastore
	if !found {
		if err := c.DSClient.Get(ctx, key, dst); err != nil {
			return fmt.Errorf("dscache.Client.Get: failed datastore get %v", err)
		}
		if err := c.putCache(key, dst); err != nil {
			// todo can i ignore this?
			log.Printf(err.Error())
		}
	}

	return nil
}

// GetMulti is batch version of Get
func (c *Client) GetMulti(ctx context.Context, keys []*datastore.Key, dst interface{}) error {
	vals := reflect.ValueOf(dst)

	if vals.Kind() != reflect.Slice {
		return fmt.Errorf("dscache.Client.GetMulti: dst must be slice of pointers")
	}

	if len(keys) != vals.Len() {
		return fmt.Errorf("dscache.Client.GetMulti: keys and dst must be same length")
	}

	var findInCacheKeys []*datastore.Key
	var findInCacheVals []interface{}
	var findInDSKeys []*datastore.Key
	var findInDSVals []interface{}
	var cKeys []string

	// check local cache first
	c.localCacheLock.RLock()
	for idx, key := range keys {
		val := vals.Index(idx).Interface()
		set := reflect.ValueOf(val)
		if set.Kind() != reflect.Ptr {
			return fmt.Errorf("dscache.Client.GetMulti: dst value is not a slice %v", val)
		}
		if v, ok := c.localCache[cacheKey(keys[idx])]; ok {
			if !set.CanSet() {
				set = set.Elem()
			}
			set.Set(reflect.Indirect(reflect.ValueOf(v)))
		} else {
			findInCacheKeys = append(findInCacheKeys, key)
			findInCacheVals = append(findInCacheVals, val)
			cKeys = append(cKeys, cacheKey(key))
		}
	}
	c.localCacheLock.RUnlock()

	// check redis cache
	if len(findInCacheKeys) > 0 {
		if err := c.Cache.GetMulti(cKeys, findInCacheVals); err != nil {
			// some are found or all failed
			if merr, ok := err.(MultiError); ok {
				for idx, key := range findInCacheKeys {
					val := findInCacheVals[idx]
					if merr[idx] == nil {
						c.putLocalCache(key, val)
					} else {
						findInDSKeys = append(findInDSKeys, key)
						findInDSVals = append(findInDSVals, val)
					}
				}
			} else {
				// failed to get from cache
				findInDSKeys = findInCacheKeys
				findInDSVals = findInCacheVals
			}
		} else {
			// all found
			for idx, key := range findInCacheKeys {
				c.putLocalCache(key, findInCacheVals[idx])
			}
		}
	}

	// find in datastore
	if len(findInDSKeys) > 0 {
		// var merr datastore.MultiError
		keyErrs := make(map[*datastore.Key]error)
		var toCache map[string]interface{}
		if err := c.DSClient.GetMulti(ctx, findInDSKeys, findInDSVals); err != nil {
			if merr, ok := err.(datastore.MultiError); ok {
				for idx, key := range findInDSKeys {
					keyErrs[key] = merr[idx]
					if merr[idx] == nil {
						val := findInDSVals[idx]
						toCache[cacheKey(key)] = val
						c.putLocalCache(key, val)
					}
				}
			}
		}

		if len(toCache) > 0 {
			if err := c.Cache.SetMulti(toCache, c.Expire); err != nil {
				// todo handle failed to cache
				log.Printf(err.Error())
			}
		}

		if len(keyErrs) > 0 {
			merr := make(datastore.MultiError, len(keys))
			for idx, key := range keys {
				if err, ok := keyErrs[key]; ok {
					merr[idx] = err
				} else {
					merr[idx] = nil
				}
			}
			return merr
		}
	}

	return nil
}

// Delete removes from cache then datastore
func (c *Client) Delete(ctx context.Context, key *datastore.Key) error {
	cKey := cacheKey(key)
	errc := make(chan error)
	go func() {
		errc <- c.Cache.Delete(cKey)
	}()
	c.localCacheLock.Lock()
	if _, ok := c.localCache[cKey]; ok {
		delete(c.localCache, cKey)
	}
	c.localCacheLock.Unlock()
	if err := <-errc; err != nil {
		return fmt.Errorf("dscache.Client.Delete: failed to delete from cache %v", err)
	}

	// Delete data from datastore.
	if err := c.DSClient.Delete(ctx, key); err != nil {
		return fmt.Errorf("dscache.Client.Delete: failed to delete from datastore %v", err)
	}

	return nil
}

// DeleteMulti batch version of Delete
func (c *Client) DeleteMulti(ctx context.Context, keys []*datastore.Key) error {
	cKeys := make([]string, len(keys))
	errc := make(chan error)
	go func() {
		errc <- c.Cache.Delete(cKeys...)
	}()
	c.localCacheLock.Lock()
	for _, cKey := range cKeys {
		if _, ok := c.localCache[cKey]; ok {
			delete(c.localCache, cKey)
		}
	}
	c.localCacheLock.Unlock()
	if err := <-errc; err != nil {
		return fmt.Errorf("dscache.Client.DeleteMulti: failed to delete from cache %v", err)
	}

	// Delete data from datastore.
	if err := c.DSClient.DeleteMulti(ctx, keys); err != nil {
		return fmt.Errorf("dscache.Client.DeleteMulti: failed to delete from datastore %v", err)
	}

	return nil
}

// FlushLocal removes all local cache
func (c *Client) FlushLocal() {
	c.localCacheLock.Lock()
	defer c.localCacheLock.Unlock()
	c.localCache = make(map[string]interface{})
}

// FlushAll removes all cache
func (c *Client) FlushAll() error {
	c.FlushLocal()
	return c.Cache.Flush()
}

func cacheKey(k *datastore.Key) string {
	return fmt.Sprintf("%s%s", cacheVersionPrefix, k.Encode())
}
