`jupiter` is our custom [Redis](https://redis.io/) mini-clone designed specifically for intensive caching (designed specifically for the [`MSET`](https://redis.io/commands/mset/)/[`MGET`](https://redis.io/commands/mget/) commands), and scalability. It acts as a proxy to multiple Redis (in our case, [Memorystore](https://cloud.google.com/memorystore)) nodes and handles load distribution using [consistent hashing](https://en.wikipedia.org/wiki/Consistent_hashing).
