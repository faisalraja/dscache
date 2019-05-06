package dscache

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"reflect"
	"strings"
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

func (c *Client) deleteLocalCache(keys ...string) {
	c.localCacheLock.Lock()
	defer c.localCacheLock.Unlock()

	for _, key := range keys {
		if _, ok := c.localCache[key]; ok {
			delete(c.localCache, key)
		}
	}
}

func (c *Client) putMultiCache(items map[string]interface{}) error {
	errc := make(chan error)
	go func() {
		if len(items) == 1 {
			for k, v := range items {
				errc <- c.Cache.Set(k, marshal(v), c.Expire)
			}
		} else {
			errc <- c.Cache.SetMulti(marshalMap(items), c.Expire)
		}
	}()
	c.putMultiLocalCache(items)
	if err := <-errc; err != nil {
		// todo some sort of retry
		return fmt.Errorf("dscache.Client.putCache: failed to cache %v", err)
	}
	return nil
}

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
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("dscache.Client.Get: dst must be a pointer")
	}
	found := false
	cKey := cacheKey(key)

	// Check local cache first
	c.localCacheLock.RLock()
	if val, ok := c.localCache[cKey]; ok {
		setValue(key, v, val)
		found = true
	}
	c.localCacheLock.RUnlock()

	// Check redis cache
	if !found {
		if buf, err := c.Cache.Get(cKey); err == nil {
			setValue(key, v, buf)
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
		cKeys       []string
		findInCache []int
		findInDS    []int
		dsKeys      []*datastore.Key
	)
	toCache := make(map[string]interface{})
	keyErrs := make(map[*datastore.Key]error)

	// check local cache first
	c.localCacheLock.RLock()
	for i := range keys {
		elem := vals.Index(i)
		cKey := cacheKey(keys[i])
		if v, ok := c.localCache[cKey]; ok {
			setValue(keys[i], elem, v)
		} else {
			findInCache = append(findInCache, i)
			cKeys = append(cKeys, cKey)
		}
	}
	c.localCacheLock.RUnlock()

	// check redis cache
	if len(findInCache) > 0 {
		if bMap, err := c.Cache.GetMulti(cKeys...); err != nil {
			// some are found or all failed
			if merr, ok := err.(MultiError); ok {
				for _, i := range findInCache {
					var serr error
					key := keys[i]
					cKey := cacheKey(key)
					val := vals.Index(i)
					if merr.Key(cKey) == nil {
						serr := setValue(key, val, bMap[cKey])
						if serr == nil {
							toCache[cKey] = val.Interface()
						}
					} else {
						serr = merr
					}

					if serr != nil {
						dsKeys = append(dsKeys, key)
						findInDS = append(findInDS, i)
					}
				}
			} else {
				// failed to get from cache
				findInDS = findInCache
				for _, i := range findInCache {
					dsKeys = append(dsKeys, keys[i])
				}
			}
		} else {
			// all found
			for _, i := range findInCache {
				key := keys[i]
				cKey := cacheKey(key)
				val := vals.Index(i)
				serr := setValue(key, val, bMap[cKey])
				if serr == nil {
					toCache[cKey] = val.Interface()
				}
			}
		}

		if len(toCache) > 0 {
			c.putMultiLocalCache(toCache)
			toCache = make(map[string]interface{})
		}
	}

	// find in datastore
	if len(findInDS) > 0 {
		dsVals := reflect.MakeSlice(reflect.TypeOf(dst), len(dsKeys), len(dsKeys))
		for k, i := range findInDS {
			dsVals.Index(k).Set(vals.Index(i))
		}
		cKeys = make([]string, 0)
		if err := c.DSClient.GetMulti(ctx, dsKeys, dsVals.Interface()); err != nil {
			if merr, ok := err.(datastore.MultiError); ok {
				for i, key := range dsKeys {
					keyErrs[key] = merr[i]
					if merr[i] == nil {
						cKey := cacheKey(key)
						cKeys = append(cKeys, cKey)
						toCache[cKey] = dsVals.Index(i).Interface()
					}
				}
			}
		} else {
			for i, key := range dsKeys {
				cKey := cacheKey(key)
				cKeys = append(cKeys, cKey)
				toCache[cKey] = dsVals.Index(i).Interface()
			}
		}
		// re-assign results to dst
		for k, i := range findInDS {
			vals.Index(i).Set(dsVals.Index(k))
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
		realErrs := 0
		merr := make(datastore.MultiError, len(keys))
		for i, key := range keys {
			err := keyErrs[key]
			// we consider no such entity as not error
			if err == datastore.ErrNoSuchEntity {
				merr[i] = nil
			} else {
				merr[i] = err

				if err != nil {
					realErrs++
				}
			}
		}
		if realErrs > 0 {
			return merr
		}
	}

	return nil
}

// Delete removes from datastore then cache
func (c *Client) Delete(ctx context.Context, key *datastore.Key) error {
	return c.DeleteMulti(ctx, []*datastore.Key{key})
}

// DeleteMulti batch version of Delete
func (c *Client) DeleteMulti(ctx context.Context, keys []*datastore.Key) error {
	if err := c.DSClient.DeleteMulti(ctx, keys); err != nil {
		return fmt.Errorf("dscache.Client.DeleteMulti: failed to delete %v", err)
	}

	cKeys := make([]string, len(keys))
	for i, key := range keys {
		cKeys[i] = cacheKey(key)
	}
	// Delete data from cache
	c.deleteLocalCache(cKeys...)
	if err := c.Cache.Delete(cKeys...); err != nil {
		log.Printf("dscache.Client.DeleteMulti: failed to delete from cache %v", err)
	}

	return nil
}

// RunInTransaction runs a datastore transaction
func (c *Client) RunInTransaction(ctx context.Context, f func(tx *Transaction) error, opts ...datastore.TransactionOption) error {
	var txn *Transaction

	_, err := c.DSClient.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		txn = &Transaction{
			txn:       tx,
			deleteMap: make(map[string]bool),
		}
		return f(txn)
	}, opts...)

	if err == nil {
		txn.txnLock.Lock()
		defer txn.txnLock.Unlock()
		if len(txn.deleteMap) > 0 {
			var keys []string
			for key := range txn.deleteMap {
				keys = append(keys, key)
			}
			c.deleteLocalCache(keys...)
			c.Cache.Delete(keys...)
		}
	} else {
		log.Printf("dscache.Client.RunInTransaction: %v", err)
	}

	return err
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

func loadKey(key *datastore.Key, v reflect.Value) {
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()

	// find datastore:"__key__"
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("datastore")
		if strings.Contains(tag, "__key__") {
			fv := v.FieldByName(f.Name)
			if fv.Kind() == reflect.Ptr {
				fv.Set(reflect.ValueOf(key))
			}
			break
		}
	}
}

func marshal(d interface{}) []byte {
	buf := bytes.Buffer{}
	if err := gob.NewEncoder(&buf).Encode(d); err != nil {
		log.Fatalf("dscache.marshal: error %v", err)
		return nil
	}
	return buf.Bytes()
}

func marshalMap(m map[string]interface{}) map[string][]byte {
	o := make(map[string][]byte, len(m))
	for k, v := range m {
		o[k] = marshal(v)
	}
	return o
}

func unmarshal(data []byte, dst interface{}) error {
	return gob.NewDecoder(bytes.NewBuffer(data)).Decode(dst)
}

func setValue(key *datastore.Key, val reflect.Value, d interface{}) error {

	if val.Kind() == reflect.Ptr && val.IsNil() {
		val.Set(reflect.New(val.Type().Elem()))
	}

	if buf, ok := d.([]byte); ok {
		if err := unmarshal(buf, val.Interface()); err != nil {
			return err
		}
	} else {
		if !val.CanSet() {
			val = val.Elem()
		}
		vv := reflect.ValueOf(d)
		if vv.Type() == val.Type() {
			val.Set(vv)
		} else if vv.Kind() != reflect.Ptr {
			var vt reflect.Value
			if vv.CanAddr() {
				vt = vv.Addr()
			} else {
				vt = reflect.New(vv.Type()).Elem()
				vt.Set(vv)
				vt = vt.Addr()
			}
			val.Set(vt)
		} else {
			val.Set(reflect.Indirect(vv))
		}
	}

	if key != nil {
		loadKey(key, val)
	}

	return nil
}
