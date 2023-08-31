package flags

import (
	"flag"
)

var (
	Test              = flag.Bool("test", false, "Scratch pad, anything")
	Members           = flag.String("members", "", "Initial Redis members, comma-separated, fmt: [passwd@]host:port")
	Partitions        = flag.Int("partitions", 27_103, "Partition count for our consistent hashring")
	ReplicationFactor = flag.Int("replicationfactor", 10, "Replication factor for our consistent hashring")
	Database          = flag.String("db", "", "Spanner database, fmt: projects/{v}/instances/{v}/databases/{v}")
	MaxIdle           = flag.Int("maxidle", 3, "Maximum idle connections to jupiter")
	MaxActive         = flag.Int("maxactive", 1_000, "Maximum active connections to jupiter")
)
