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
		Key    *datastore.Key `datastore:"__key__"`
		Str    string         `datastore:"str"`
		Int    int            `datastore:"int"`
		Ignore string         `datastore:"-"`
		Byte   []byte
		Strs   []string
		Bool   bool
		Int64  int64
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
		Str:    "hello",
		Int:    1,
		Int64:  100,
		Ignore: "Yes",
		Bool:   true,
		Strs:   []string{"test a", "test b"},
	}
	k1 := datastore.IDKey("TestA", 0, nil)

	key, err := client.Put(ctx, k1, t1)
	if err != nil {
		t.Errorf("Failed to put %v", err)
	}
	c1 := cacheKey(key)
	if _, ok := client.localCache[c1]; !ok {
		t.Errorf("Failed to put to local cache")
	}
	if found, _ := client.Cache.Exists(c1); !found {
		t.Errorf("Failed to put to redis cache")
	}

	t2 := &TestA{}
	if err := client.Get(ctx, key, t2); err != nil {
		t.Errorf("Failed to get %v", err)
	}
	if !reflect.DeepEqual(t1, t2) {
		t.Errorf("t1 and t2 not equal")
	}
	client.FlushLocal()
	t2 = &TestA{}
	if err := client.Get(ctx, key, t2); err != nil {
		t.Errorf("Failed to get %v", err)
	}
	if !reflect.DeepEqual(t1, t2) {
		t.Errorf("t1 and t2 not equal")
	}
	client.FlushAll()
	t2 = &TestA{}
	if err := client.Get(ctx, key, t2); err != nil {
		t.Errorf("Failed to get %v", err)
	}
	// should not be equal since we don't load the Key here but still pass above
	if reflect.DeepEqual(t1, t2) {
		t.Errorf("t1 and t2 not equal")
	}
}

func TestPutMultiGetMulti(t *testing.T) {
	ctx := context.Background()
	client := NewClient(ctx, testDSClient, testCache)

	t1 := &TestA{
		Str:    "hello",
		Int:    1,
		Int64:  100,
		Ignore: "Yes",
		Bool:   true,
		Strs:   []string{"test a", "test b"},
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
	// client.FlushAll()
	keys = append(keys, k4, k5)
	out := make([]*TestA, 5)

	if err := client.GetMulti(ctx, keys, out); err != nil {
		if merr, ok := err.(datastore.MultiError); ok {
			if merr[0] != nil {
				t.Errorf("Failed error 0")
			}
			if merr[1] != nil {
				t.Errorf("Failed error 1")
			}
			if merr[2] != nil {
				t.Errorf("Failed error 2")
			}
			if merr[3] == nil {
				t.Errorf("Failed error 3")
			}
			if merr[4] == nil {
				t.Errorf("Failed error 4")
			}

		}
	}
	client.FlushAll()
}

func TestRunQueryDeleteAll(t *testing.T) {
	ctx := context.Background()
	client := NewClient(ctx, testDSClient, testCache)

	t1 := &TestA{
		Str:    "hello",
		Int:    1,
		Int64:  100,
		Ignore: "Yes",
		Bool:   true,
		Strs:   []string{"test a", "test b"},
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
