`jupiter` is our custom, highly-scalable [Redis](https://redis.io/) mini-clone specifically designed as cache for our graph and reports data. It acts as a proxy to a cluster of Redis (in our case, [Memorystore](https://cloud.google.com/memorystore)) nodes and handles load distribution using [consistent hashing](https://en.wikipedia.org/wiki/Consistent_hashing). Sort of similar to [Envoy Redis proxy](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/other_protocols/redis). It supports (WIP) dynamic addition and removal of Redis nodes with minimal interruptions.

`jupiter` is not a Redis replacement, although it is compatible with most Redis clients such as [`redigo`](https://github.com/gomodule/redigo) and [`go-redis`](https://github.com/redis/go-redis). It is a best-effort caching system, prioritizing availability over consistency.

There is a slight performance penalty over direct connections to Redis due to its use of hashing; it requires all commands to have a key as hash to find the corresponding Redis node. If you require high performance caching, you're probably better off using a dedicated Redis node/cluster.

### Why

The main reason why `jupiter` exists is that at the time of this writing, the maximum Memorystore instance available in GCP is 300GB. Beyond this, we need to create more instances. Instead of leaving the responsibility of accessing multiple Memorystore instances to our applications, it's move convenient to be able to access these clusters of instances as a single entity. After a trial run with [Envoy Redis proxy](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/other_protocols/redis), we wanted something more, especially the ability to add/remove instances seamlessly with minimal interruptions to our client applications.

### Hashing

Most of the "caching" commands in Redis need a key (usually `args[1]`, or the argument after the command itself). `jupiter` will try to use this argument as the default hashing key. This renders some other Redis commands unsupported, such as cluster commands, Pub/Sub, transactions, LUA scripting, etc. However, `jupiter` also provides a custom way to input a (or override the) hashing key if needed **using the last argument**.

Adding the `hash={key}` argument at the end of a command tells `jupiter` to use `{key}` as the hash key. This argument won't be included in the final Redis command that is submitted to the target node.

For example:

```sh
# Use 'somekey' as the hashing key.
redis> SET hello world hash=somekey
redis> GET hello hash=somekey

# Here, jupiter will use 'hello' as the hashing key.
redis> SET hello world
redis> GET hello

# For scans, you most definitely want to provide the hash.
redis> MSET key1 val1 key2 val2 key3 val3 hash=somekey
"OK"
redis> SCAN 0 MATCH key* hash=somekey
1) "0"
2) 1) "key3"
   2) "key2"
   3) "key1"
```

Finally, `jupiter` will use a random hash key if none is detected/provided. For example, commands with no arguments such as `DBSIZE`, `TIME`, `RANDOMKEY`, etc.

### Usage

```go
import (
    ...
    "github.com/gomodule/redigo/redis"
)

hashKey := "hash=sample/hashkey"
pool := &redis.Pool{
    MaxIdle:     3,
    MaxActive:   100,
    IdleTimeout: 240 * time.Second,
    Dial: func() (redis.Conn, error) {
        return redis.Dial("tcp", "jupiter-redis.default.svc.cluster.local:6379")
    },
}

con := pool.Get()
defer con.Close()
v, err := redis.String(con.Do("PING", hashKey))
if err != nil {
    log.Printf("PING failed: %v", err)
} else {
    log.Printf("reply=%v", v)
}
```

### Try it out (internal)

If you want to try `jupiter` using `redis-cli`, you can do so by:

```sh
# Do a port-forward of the prod service to your local (separate terminal):
$ brew install flowerinthenight/tap/kubepfm
$ kubepfm --target service/jupiter-redis:6379:6379

# Connect redis-cli to the forwarded port (separate terminal):
$ redis-cli
127.0.0.1:6379> PING hash=somekey
"PONG"
127.0.0.1:6379> quit
```

### Use case(s)

At the moment, our [TrueUnblended Engine](https://labs.alphaus.cloud/docs/trueunblended/) and some of our [streaming APIs](https://labs.alphaus.cloud/blueapidocs/#/Cost) use `jupiter` to cache graph and reports data into multiple chunks (64KB by default) which are distributed across its cluster. `jupiter` exposes a [filesystem-like API](https://github.com/mobingilabs/ouchan/tree/master/pkg/jupiter) that clients can use to read and write files (or data blobs) to a cluster. Each data will have a name (filename) and once written, its data chunks will be distributed across the cluster. During retrieval, `jupiter` handles the assembly of all data chunks from the cluster and returns it to the caller as a single file (blob). Something like:

```go
err := jupiter.WriteAll("file1", []byte("data bigger than 64KB"))
if err != nil {
    return
}
...
b, err := jupiter.ReadAll("file1")
if err != nil {
    return
}
// do something with b
```

### Limitations

Pipelining and transactions are not supported as `jupiter` doesn't guarantee the use of a single connection for multiple, related commands (i.e. `MULTI`, `...`, `EXEC`), even if the same hash key is provided. We might support these in future versions.
