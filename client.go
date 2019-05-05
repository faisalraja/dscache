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
func NewClient(ctx context.Context, dsClient *datastore.Client, cache *Cache) *Client {
	return &Client{
		Context:    ctx,
		DSClient:   dsClient,
		Cache:      cache,
		Expire:     time.Second * 0,
		localCache: make(map[string]interface{}),
	}
}

func (c *Client) putMultiLocalCache(items map[string]interface{}) {
	c.localCacheLock.Lock()
	defer c.localCacheLock.Unlock()
	for k, v := range items {
		c.localCache[k] = v
	}
}

func (c *Client) putLocalCache(key string, value interface{}) {
	items := make(map[string]interface{})
	items[key] = value
	c.putMultiLocalCache(items)
}

func (c *Client) putCache(key string, src interface{}) error {
	items := make(map[string]interface{})
	items[key] = src
	return c.putMultiCache(items)
}

func (c *Client) putMultiCache(items map[string]interface{}) error {
	errc := make(chan error)
	go func() {
		if len(items) == 1 {
			for k, v := range items {
				errc <- c.Cache.Set(k, v, c.Expire)
			}
		} else {
			errc <- c.Cache.SetMulti(items, c.Expire)
		}
	}()
	c.putMultiLocalCache(items)
	if err := <-errc; err != nil {
		// todo some sort of retry
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
	cKey := cacheKey(key)
	if err := c.putCache(cKey, src); err != nil {
		// we try to delete on fail to put to avoid stale data
		if err2 := c.Cache.Delete(cKey); err2 != nil {
			log.Printf("dscache.Client.Put: permanent failure possible stale data for key: %v err: %v delerr: %v", key, err, err2)
		}
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
	var cKeys []string
	for idx, key := range keys {
		cKey := cacheKey(key)
		valMap[cKey] = vals.Index(idx).Interface()
		cKeys = append(cKeys, cKey)
	}
	if err := c.putMultiCache(valMap); err != nil {
		// delete on failure to put into cache
		if err2 := c.Cache.Delete(cKeys...); err2 != nil {
			log.Printf("dscache.Client.PutMulti: permanent failure possible stale data for keys: %v err: %v delerr: %v", keys, err, err2)
		}
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
			c.putLocalCache(cKey, dst)
			found = true
		}
	}

	// get from datastore
	if !found {
		if err := c.DSClient.Get(ctx, key, dst); err != nil {
			return fmt.Errorf("dscache.Client.Get: failed datastore get %v", err)
		}
		if err := c.putCache(cKey, dst); err != nil {
			return fmt.Errorf("dscache.Client.Get: failed to put in cache %v", err)
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

	var (
		findInCacheKeys []*datastore.Key
		findInCacheVals []interface{}
		findInDSKeys    []*datastore.Key
		findInDSVals    []interface{}
		cKeys           []string
	)
	toCache := make(map[string]interface{})

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
						toCache[cacheKey(key)] = val
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
				toCache[cacheKey(key)] = findInCacheVals[idx]
			}
		}

		if len(toCache) > 0 {
			c.putMultiLocalCache(toCache)
			toCache = make(map[string]interface{})
		}
	}

	// find in datastore
	if len(findInDSKeys) > 0 {
		cKeys = make([]string, 0)
		keyErrs := make(map[*datastore.Key]error)
		if err := c.DSClient.GetMulti(ctx, findInDSKeys, findInDSVals); err != nil {
			if merr, ok := err.(datastore.MultiError); ok {
				for idx, key := range findInDSKeys {
					keyErrs[key] = merr[idx]
					if merr[idx] == nil || merr[idx] == datastore.ErrNoSuchEntity {
						cKey := cacheKey(key)
						toCache[cKey] = findInDSVals[idx]
						cKeys = append(cKeys, cKey)
					}
				}
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

		if len(toCache) > 0 {
			if err := c.putMultiCache(toCache); err != nil {
				// delete on failure to put into cache
				if err2 := c.Cache.Delete(cKeys...); err2 != nil {
					log.Printf("dscache.Client.GetMulti: permanent failure possible stale data for keys: %v err: %v delerr: %v", cKeys, err, err2)
				}
			}
		}
	}

	return nil
}

// Delete removes from cache then datastore
func (c *Client) Delete(ctx context.Context, key *datastore.Key) error {
	cKey := cacheKey(key)

	if err := c.DSClient.Delete(ctx, key); err != nil {
		return fmt.Errorf("dscache.Client.Delete: failed to delete %v", err)
	}

	c.localCacheLock.Lock()
	if _, ok := c.localCache[cKey]; ok {
		delete(c.localCache, cKey)
	}
	c.localCacheLock.Unlock()

	if err := c.Cache.Delete(cKey); err != nil {
		log.Printf("dscache.Client.Delete: failed to delete from cache err: %v", err)
	}

	return nil
}

// DeleteMulti batch version of Delete
func (c *Client) DeleteMulti(ctx context.Context, keys []*datastore.Key) error {
	cKeys := make([]string, len(keys))

	if err := c.DSClient.DeleteMulti(ctx, keys); err != nil {
		return fmt.Errorf("dscache.Client.DeleteMulti: failed to delete %v", err)
	}

	c.localCacheLock.Lock()
	for _, cKey := range cKeys {
		if _, ok := c.localCache[cKey]; ok {
			delete(c.localCache, cKey)
		}
	}
	c.localCacheLock.Unlock()
	// Delete data from cache
	if err := c.Cache.Delete(cKeys...); err != nil {
		log.Printf("dscache.Client.DeleteMulti: failed to delete from cache %v", err)
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
