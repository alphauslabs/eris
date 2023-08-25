`jupiter` is our custom [Redis](https://redis.io/) mini-clone designed for intensive caching (designed specifically for the [`MSET`](https://redis.io/commands/mset/)/[`MGET`](https://redis.io/commands/mget/) commands), and scalability. It acts as a proxy to multiple Redis (in our case, [Memorystore](https://cloud.google.com/memorystore)) nodes and handles load distribution using [consistent hashing](https://en.wikipedia.org/wiki/Consistent_hashing). It supports (WIP) dynamic addition and removal of Redis nodes with minimal interruptions.

This is not a Redis server replacement, although it is compatible with most Redis clients such as [`redigo`](https://github.com/gomodule/redigo).

There is a performance penalty over direct connections to Redis due to its use of hashing; it requires all commands to have a key as hash to find the corresponding Redis node. Thus, if you require high performance caching, you're better off using a direct connection to a separate Redis node/cluster.
