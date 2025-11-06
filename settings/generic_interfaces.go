package settings

type GenericKafkaTopicSpecification struct {
	// Topic name to create.
	Topic string
	// Number of partitions in topic.
	NumPartitions int
	// Default replication factor for the topic's partitions, or zero
	// if an explicit ReplicaAssignment is set.
	ReplicationFactor int
	// (Optional) Explicit replica assignment. The outer array is
	// indexed by the partition number, while the inner per-partition array
	// contains the replica broker ids. The first broker in each
	// broker id list will be the preferred replica.
	ReplicaAssignment [][]int32
	// Topic configuration.
	Config map[string]string
}
