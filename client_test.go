package dscache

import (
	"context"
	"reflect"
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
}

func TestNewClient(t *testing.T) {
	ctx := context.Background()

	_, err := NewClient(ctx, testDSClient, testCache)
	if err != nil {
		t.Errorf("Failed to create dscache client: %v", err)
	}
}

func TestPut(t *testing.T) {
	ctx := context.Background()
	client, _ := NewClient(ctx, testDSClient, testCache)

	t1 := &TestA{}
	k1 := datastore.IDKey("TestA", 0, nil)
	key, err := client.Put(ctx, k1, t1)
	if err != nil {
		t.Errorf("Failed put: %v", err)
	}

	c1 := cacheKey(key)
	if _, ok := client.localCache[c1]; !ok {
		t.Errorf("Failed to put to local cache")
	}

	if found, _ := client.Cache.Exists(c1); !found {
		t.Errorf("Failed to put to redis")
	}

	// when recreating a context local cache should be empty
	ctx = context.Background()
	client, _ = NewClient(ctx, testDSClient, testCache)
	if _, ok := client.localCache[c1]; ok {
		t.Errorf("Local cache should not exists from new ctx")
	}
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	client, _ := NewClient(ctx, testDSClient, testCache)

	t1 := &TestA{
		Str:    "hello",
		Int:    1,
		Int64:  100,
		Ignore: "Yes",
		Bool:   true,
		Strs:   []string{"test a", "test b"},
	}
	k1 := datastore.IDKey("TestA", 0, nil)
	key, _ := client.Put(ctx, k1, t1)

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
