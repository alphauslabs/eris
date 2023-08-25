`jupiter` is our custom [Redis](https://redis.io/) mini-clone designed for intensive caching (designed specifically for the [`MSET`](https://redis.io/commands/mset/)/[`MGET`](https://redis.io/commands/mget/) commands), and scalability. It acts as a proxy to a cluster of Redis (in our case, [Memorystore](https://cloud.google.com/memorystore)) nodes and handles load distribution using [consistent hashing](https://en.wikipedia.org/wiki/Consistent_hashing). Sort of similar to [Envoy Redis proxy](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/other_protocols/redis). It supports (WIP) dynamic addition and removal of Redis nodes with minimal interruptions.

This is not a Redis server replacement, although it is compatible with most Redis clients such as [`redigo`](https://github.com/gomodule/redigo). It is a best-effort caching system, prioritizing availability over consistency.

There is a performance penalty over direct connections to Redis due to its use of hashing; it requires all commands to have a key as hash to find the corresponding Redis node. Thus, if you require high performance caching, you're probably better off using a separate Redis node/cluster.

**Hashing**

Most of the "caching" commands in Redis need a key (usually the argument after the command itself, i.e. `GET {key} ...`). `jupiter` will try to use this argument as the default hashing key, therefore some other commands are not supported, such as cluster commands, Pub/Sub, transactions, LUA scripting, etc. However, `jupiter` also provides a custom way to override the hashing key if needed. Since the equivalent node for a hash doesn't really change (unless the cluster membership changes), this is useful if you want a guarantee to have the same node handling your commands.

```sh
# Option 1:
# Adding the hash={key} argument at the end.
# This argument won't be included in the actual Redis command.
redis> SET hello world hash=samplehash
redis> GET hello hash=samplehash

# This is actually the same as the following since
# jupiter will use the argument 'hello' as the hash.
redis> SET hello world
redis> GET hello
```

```sh
# Option 2:
# Adding the index={num} argument at the end.
# This argument won't be included in the actual Redis command.
# The {num} refers to the 0-based index of the command to be used as key.
```

**Example**

TBD
