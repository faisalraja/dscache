package dscache

import (
	"context"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"testing"

	"cloud.google.com/go/datastore"
)

// Run datastore emulator and redis then set these environment variables
// DATASTORE_EMULATOR_HOST = localhost:8030
// REDIS_HOST = :6378
// to run datastore
// gcloud beta emulators datastore start --no-store-on-disk

var testCache *Cache
var testDSClient *datastore.Client

type (
	TestA struct {
		Key     *datastore.Key `datastore:"__key__"`
		Str     string         `datastore:"str"`
		Int     int            `datastore:"int"`
		Nil     *string        `datastore:"noindex,omitempty"`
		Strs    []string
		Bool    bool
		Int64   int64
		Float64 float64
	}

	TestB struct {
		ID  int64
		A   TestA
		B   []TestA
		IDs []int64
	}
)

func init() {
	ctx := context.Background()
	testCache = NewCache("")
	if client, err := datastore.NewClient(ctx, "dscache"); err != nil {
		panic(err)
	} else {
		testDSClient = client
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	signal.Notify(c, syscall.SIGKILL)
	go func() {
		<-c
		testCache.Pool.Close()
	}()
}

func TestPutGet(t *testing.T) {
	ctx := context.Background()
	client := NewClient(ctx, testDSClient, testCache)

	t1 := &TestA{
		Str:   "hello",
		Int:   1,
		Int64: 100,
		Bool:  true,
		Strs:  []string{"test a", "test b"},
	}
	k1 := datastore.IDKey("TestA", 0, nil)

	key, err := client.Put(ctx, k1, t1)
	if err != nil {
		t.Errorf("Failed to put %v", err)
	}
	c1 := cacheKey(key)
	cv1, ok := client.localCache[c1]
	loadKey(key, reflect.ValueOf(cv1))
	if !ok {
		t.Errorf("Failed to put to local cache")
	}
	if !reflect.DeepEqual(cv1, t1) {
		t.Errorf("Value does not match from local cache")
	}
	t2 := &TestA{}
	if buf, err := client.Cache.Get(c1); err != nil {
		t.Errorf("Failed to put to redis cache")
	} else {
		setValue(key, reflect.ValueOf(t2), buf)
	}
	if !reflect.DeepEqual(cv1, t2) {
		t.Errorf("Value does not match from redis cache: %v != %v", cv1, t2)
	}

	localT := &TestA{}
	if err := client.Get(ctx, key, localT); err != nil {
		t.Errorf("Failed to get %v", err)
	}
	client.FlushLocal()

	redisT := &TestA{}
	if err := client.Get(ctx, key, redisT); err != nil {
		t.Errorf("Failed to get %v", err)
	}
	client.FlushAll()

	dsT := &TestA{}
	if err := client.Get(ctx, key, dsT); err != nil {
		t.Errorf("Failed to get %v", err)
	}

	if !reflect.DeepEqual(dsT, localT) {
		t.Errorf("DS value not equal to local: %v != %v", dsT, localT)
	}
	if !reflect.DeepEqual(dsT, redisT) {
		t.Errorf("DS value not equal to redis: %v != %v", dsT, redisT)
	}
}

func TestPutMultiGetMulti(t *testing.T) {
	ctx := context.Background()
	client := NewClient(ctx, testDSClient, testCache)

	t1 := &TestA{
		Str:   "hello",
		Int:   1,
		Int64: 100,
		Bool:  true,
		Strs:  []string{"test a", "test b"},
	}
	k1 := datastore.IDKey("TestA", 0, nil)
	k2 := datastore.NameKey("TestA", "bbb", nil)
	k3 := datastore.NameKey("TestA", "ccc", nil)
	k3.Namespace = "InC"
	k4 := datastore.NameKey("TestA", "ddd", nil)
	k5 := datastore.NameKey("TestA", "eee", nil)

	input := []*TestA{t1, t1, t1}

	keys, err := client.PutMulti(ctx, []*datastore.Key{k1, k2, k3}, input)
	if err != nil {
		t.Errorf("Failed to putmulti %v", err)
	}
	// client.FlushLocal()
	client.FlushAll()
	keys = append(keys, k4, k5)
	out := make([]*TestA, 5)
	if err := client.GetMulti(ctx, keys, out); err != nil {
		if merr, ok := err.(datastore.MultiError); ok {
			for i, v := range merr {
				if i < 3 && v == nil {
					t.Errorf("MultiError %v is nil", v)
				} else if i >= 3 && v != nil {
					t.Errorf("MultiError %v is not nil", v)
				}
			}

		}
	} else {
		for i, v := range out {
			if i < 3 && v == nil {
				t.Errorf("Value %v is nil", v)
			} else if i >= 3 && v != nil {
				t.Errorf("Value %v is not nil", v)
			}
		}
	}
	client.FlushAll()
}

func TestRunQueryDeleteAll(t *testing.T) {
	ctx := context.Background()
	client := NewClient(ctx, testDSClient, testCache)

	t1 := &TestA{
		Str:   "hello",
		Int:   1,
		Int64: 100,
		Bool:  true,
		Strs:  []string{"test a", "test b"},
	}
	k1 := datastore.IDKey("TestA", 0, nil)
	k2 := datastore.IDKey("TestA", 0, nil)
	k3 := datastore.IDKey("TestA", 0, nil)
	k4 := datastore.IDKey("TestA", 0, nil)
	k5 := datastore.IDKey("TestA", 0, nil)

	input := []*TestA{t1, t1, t1, t1, t1}

	_, err := client.PutMulti(ctx, []*datastore.Key{k1, k2, k3, k4, k5}, input)
	if err != nil {
		t.Errorf("Failed to putmulti %v", err)
	}
	out := make([]*TestA, 3)
	q := datastore.NewQuery("TestA")
	var cursor string
	for {
		keys, cursor, err := client.RunQuery(ctx, q, out, cursor)
		if err != nil {
			t.Errorf("Failed to run query %v", err)
		}

		if len(keys) > 0 {
			if err := client.DeleteMulti(ctx, keys); err != nil {
				t.Errorf("Failed to delete keys: %v", len(keys))
			}
		}

		if cursor == "" {
			break
		}
	}

	client.FlushAll()
}
