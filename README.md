`jupiter` is our custom [Redis](https://redis.io/) mini-clone designed for intensive caching (designed specifically for the [`MSET`](https://redis.io/commands/mset/)/[`MGET`](https://redis.io/commands/mget/) commands), and scalability. It acts as a proxy to a cluster of Redis (in our case, [Memorystore](https://cloud.google.com/memorystore)) nodes and handles load distribution using [consistent hashing](https://en.wikipedia.org/wiki/Consistent_hashing). Sort of similar to [Envoy Redis proxy](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/other_protocols/redis). It supports (WIP) dynamic addition and removal of Redis nodes with minimal interruptions.

This is not a Redis server replacement, although it is compatible with most Redis clients such as [`redigo`](https://github.com/gomodule/redigo). It is a best-effort caching system, prioritizing availability over consistency.

There is a performance penalty over direct connections to Redis due to its use of hashing; it requires all commands to have a key as hash to find the corresponding Redis node. If you require high performance caching, you're probably better off using a separate Redis node/cluster.

### Hashing

Most of the "caching" commands in Redis need a key (usually `args[1]`, or the argument after the command itself). `jupiter` will try to use this argument as the default hashing key. This renders some other commands unsupported, such as cluster commands, Pub/Sub, transactions, LUA scripting, etc. To address this, `jupiter` also provides a custom way to provide a (or override the) hashing key if needed **using the last argument**.

**Option 1**: `hash={key}`

Adding the `hash={key}` argument at the end of a command tells `jupiter` to use `{key}` as the hash. This argument won't be included in the actual Redis command.

```sh
redis> SET hello world hash=samplehash
redis> GET hello hash=samplehash

# This is actually the same as the following since jupiter
# will use the argument 'hello' as the hash by default.
redis> SET hello world
redis> GET hello
```

```sh
# Option 2:
# Adding the index={num} argument at the end.
# This argument won't be included in the actual Redis command.
# The {num} refers to the 0-based index of the command to be used as key.
```

### Example

TBD
