package dscache

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
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

// RunQuery will execute your query in keys only and store to dst results while returning
// a cursor for the next page. The length of dst will also override the limit for your returned query.
// If dst is nil, provide your own length by setting it in query like q.Limit(10)
func (c *Client) RunQuery(ctx context.Context, q *datastore.Query, dst interface{}, cursor string) ([]*datastore.Key, string, error) {

	var keys []*datastore.Key
	var val reflect.Value
	q = q.KeysOnly()
	var limit int

	if dst != nil {
		val = reflect.ValueOf(dst)

		if val.Kind() != reflect.Slice {
			return keys, "", fmt.Errorf("dscache.Client.RunQuery: dst must be a slice of pointers")
		}
		limit = val.Len()
		q = q.Limit(limit)
	}

	if cursor != "" {
		c, err := datastore.DecodeCursor(cursor)
		if err != nil {
			return keys, "", fmt.Errorf("dscache.Client.RunQuery: invalid cursor %v", err)
		}
		q = q.Start(c)
	}
	t := c.DSClient.Run(ctx, q)

	for {
		if key, err := t.Next(nil); err == iterator.Done {
			break
		} else if err != nil {
			return keys, "", fmt.Errorf("dscache.Client.RunQuery: iterator error %v", err)
		} else {
			keys = append(keys, key)
		}
	}

	crsr, err := t.Cursor()
	if err != nil {
		return keys, "", fmt.Errorf("dscache.Client.RunQuery: cursor error %v", err)
	}
	kLen := len(keys)
	if kLen > 0 && kLen <= limit {
		cursor = crsr.String()
	} else {
		cursor = ""
	}
	if kLen > 0 && dst != nil {
		var ns reflect.Value
		if kLen < limit {
			// create right size slice and let's just fill dst later
			ns = reflect.MakeSlice(val.Type(), kLen, kLen)
			dst = ns.Interface()
			cursor = ""
		}
		if err := c.GetMulti(ctx, keys, dst); err != nil {
			return keys, cursor, fmt.Errorf("dscache.Client.RunQuery: failed to populate dst %v", err)
		}
		if ns.Kind() == reflect.Slice {
			for idx := range keys {
				val.Index(idx).Set(ns.Index(idx))
			}
		}
	}
	return keys, cursor, nil
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
	elem := reflect.ValueOf(dst)
	if elem.Kind() != reflect.Ptr {
		return fmt.Errorf("dscache.Client.Get: dst must be a pointer")
	}
	if !elem.CanSet() {
		elem = elem.Elem()
	}
	found := false
	cKey := cacheKey(key)

	// Check local cache first
	c.localCacheLock.RLock()
	if val, ok := c.localCache[cKey]; ok {
		elem.Set(reflect.Indirect(reflect.ValueOf(val)))
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
// todo support []T since cache.GetMulti doesn't
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
		findInDSKeys    []*datastore.Key
		cKeys           []string
	)
	kLen := len(keys)
	findInCacheVals := reflect.MakeSlice(vals.Type(), 0, kLen)
	findInDSVals := reflect.MakeSlice(vals.Type(), 0, kLen)
	toCache := make(map[string]interface{})
	keyErrs := make(map[*datastore.Key]error)

	// check local cache first
	c.localCacheLock.RLock()
	for idx, key := range keys {
		elem := vals.Index(idx)
		if v, ok := c.localCache[cacheKey(keys[idx])]; ok {
			vOf := reflect.ValueOf(v)
			if elem.Kind() == reflect.Ptr {
				elem.Set(vOf)
			} else {
				elem.Set(reflect.Indirect(vOf))
			}
		} else {
			if elem.Kind() == reflect.Ptr && elem.IsNil() {
				elem.Set(reflect.New(elem.Type().Elem()))
			}
			findInCacheKeys = append(findInCacheKeys, key)
			findInCacheVals = reflect.Append(findInCacheVals, elem)
			cKeys = append(cKeys, cacheKey(key))
		}
	}
	c.localCacheLock.RUnlock()

	// check redis cache
	if len(findInCacheKeys) > 0 {
		if err := c.Cache.GetMulti(cKeys, findInCacheVals.Interface()); err != nil {
			// some are found or all failed
			if merr, ok := err.(MultiError); ok {
				for idx, key := range findInCacheKeys {
					val := findInCacheVals.Index(idx)
					if merr[idx] == nil {
						toCache[cacheKey(key)] = val
					} else {
						findInDSKeys = append(findInDSKeys, key)
						findInDSVals = reflect.Append(findInDSVals, val)
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
				toCache[cacheKey(key)] = findInCacheVals.Index(idx).Interface()
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
		if err := c.DSClient.GetMulti(ctx, findInDSKeys, findInDSVals.Interface()); err != nil {
			if merr, ok := err.(datastore.MultiError); ok {
				for idx, key := range findInDSKeys {
					keyErrs[key] = merr[idx]
					if merr[idx] == nil || merr[idx] == datastore.ErrNoSuchEntity {
						cKey := cacheKey(key)
						cKeys = append(cKeys, cKey)
						toCache[cKey] = findInDSVals.Index(idx).Interface()
					}
				}
			}
		} else {
			for idx, key := range findInDSKeys {
				cKey := cacheKey(key)
				cKeys = append(cKeys, cKey)
				toCache[cKey] = findInDSVals.Index(idx).Interface()
			}
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

	if len(keyErrs) > 0 {

		// nil dst with errs?
		for idx, key := range keys {
			if err, ok := keyErrs[key]; ok && err != nil {
				elem := vals.Index(idx)
				if elem.Kind() == reflect.Ptr && !elem.IsNil() {
					elem.Set(reflect.Zero(elem.Type()))
				}
			}
		}

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
