# dscache

`This is still under heavy development and is being used in a new product that is not yet released.`

Package `github.com/faisalraja/dscache/v0` is a wrapper for [golang datastore client](https://godoc.org/cloud.google.com/go/datastore) that uses redis to cache Get,GetMulti,Put,PutMulti with strong consistency.

This is inspired by [Python NDB API](https://developers.google.com/appengine/docs/python/ndb/) where it caches to local memory then redis.

## Features

Other than the Put*, Get* methods, I'll be adding utility functions that helps what I'm using it for.

- `RunQuery` a helper function where you pass a cursor and slice to assign results. This adds KeysOnly() to your passed query and limit to length of slice that you passed.

## Usage

I recommend instanciating your datastore client and cache pool globally then create a dsclient.NewClient() per request.

```go
func main() {
    ctx := context.Background()
    // initialize your redis pool
    cache = dscache.NewCache("127.0.0.1:6379")
    defer cache.Close()
    // create your datastore client
    if dsClient, err := datastore.NewClient(ctx, "dscache"); err != nil {
        panic(err)
    }

    // in your handler somewhere
    client := dscache.NewClient(ctx, dsClient, cache)

    type User struct {
        Key *datastore.Key `datastore:"__key__"`
        Name string
        Email string
    }
    // save user
    u := &User{Name: "John Doe", Email: "jdoe@example.com"}
	userKey := datastore.IDKey("User", 0, nil)
	key, err := client.Put(ctx, userKey, u)
	if err != nil {
		// handle error
    }
    // get user
    u = &User{}
    if err := client.Get(ctx, key, u); err != nil {
        // handle error
    }
    // check u.Key, u.Name, u.Email
}
```

## Run Test

```bash
# Install and run redis by default it will use :6379 or REDIS_HOST env
# Install google cloud datastore emulator then run it
gcloud components install cloud-datastore-emulator
gcloud beta emulators datastore start --no-store-on-disk --host-port localhost:8765
export DATASTORE_EMULATOR_HOST=localhost:8765
go test
```
