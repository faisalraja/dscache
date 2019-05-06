package dscache

import (
	"sync"

	"cloud.google.com/go/datastore"
)

// Transaction for doing transactions
type Transaction struct {
	txn       *datastore.Transaction
	txnLock   sync.RWMutex
	deleteMap map[string]bool
}

func (t *Transaction) deleteKeys(keys ...*datastore.Key) {
	t.txnLock.Lock()
	defer t.txnLock.Unlock()
	for _, key := range keys {
		t.deleteMap[cacheKey(key)] = true
	}
}

// Get is the transaction-specific version of the package function Get.
func (t *Transaction) Get(key *datastore.Key, dst interface{}) (err error) {
	return t.txn.Get(key, dst)
}

// GetMulti is a batch version of Get.
func (t *Transaction) GetMulti(keys []*datastore.Key, dst interface{}) (err error) {
	return t.txn.GetMulti(keys, dst)
}

// Put is the transaction-specific version of the package function Put.
func (t *Transaction) Put(key *datastore.Key, src interface{}) (*datastore.PendingKey, error) {
	t.deleteKeys(key)
	return t.txn.Put(key, src)
}

// PutMulti is a batch version of Put. One PendingKey is returned for each
func (t *Transaction) PutMulti(keys []*datastore.Key, src interface{}) (ret []*datastore.PendingKey, err error) {
	t.deleteKeys(keys...)
	return t.txn.PutMulti(keys, src)
}

// Delete is the transaction-specific version of the package function Delete.
func (t *Transaction) Delete(key *datastore.Key) error {
	t.deleteKeys(key)
	return t.txn.Delete(key)
}

// DeleteMulti is a batch version of Delete.
func (t *Transaction) DeleteMulti(keys []*datastore.Key) (err error) {
	t.deleteKeys(keys...)
	return t.txn.DeleteMulti(keys)
}
