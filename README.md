`jupiter` is our custom [Redis](https://redis.io/) mini-clone designed for intensive caching (designed specifically for the [`MSET`](https://redis.io/commands/mset/)/[`MGET`](https://redis.io/commands/mget/) commands), and scalability. It acts as a proxy to a cluster of Redis (in our case, [Memorystore](https://cloud.google.com/memorystore)) nodes and handles load distribution using [consistent hashing](https://en.wikipedia.org/wiki/Consistent_hashing). Sort of similar to [Envoy Redis proxy](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/other_protocols/redis). It supports (WIP) dynamic addition and removal of Redis nodes with minimal interruptions.

This is not a Redis server replacement, although it is compatible with most Redis clients such as [`redigo`](https://github.com/gomodule/redigo). It is a best-effort caching system, prioritizing availability over consistency.

There is a performance penalty over direct connections to Redis due to its use of hashing; it requires all commands to have a key as hash to find the corresponding Redis node. Thus, if you require high performance caching, you're probably better off using a separate Redis node/cluster.

**Hashing**

TBD

**Example**

TBD
