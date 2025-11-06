package settings

func getBaseTopicConfig() map[string]string {
	return map[string]string{
		"retention.ms":   "-1",
		"segment.bytes":  "1073741824",
		"cleanup.policy": "compact",
	}
}

// Topic replicas/partitions can be overwritten using global defaults or by specifying particular topics
func getBaseTopicsList() []GenericKafkaTopicSpecification {
	return []GenericKafkaTopicSpecification{
		{
			Topic: "system.insert",
		},
		{
			Topic: "system.delete",
		},
		{
			Topic: "system.expedite",
			Config: map[string]string{
				"retention.ms":   "604800000",
				"segment.bytes":  "1073741824",
				"cleanup.policy": "delete",
			},
		},
		{
			Topic: "system.error",
			Config: map[string]string{
				"retention.ms":   "604800000",
				"segment.bytes":  "1073741824",
				"cleanup.policy": "delete",
			},
		},
		{
			Topic: "system.plugin",
			Config: map[string]string{
				"retention.ms":   "604800000",
				"segment.bytes":  "1073741824",
				"cleanup.policy": "delete",
			},
		},
		{
			Topic: "system.status",
			Config: map[string]string{
				"retention.ms":   "86400000",
				"segment.bytes":  "1073741824",
				"cleanup.policy": "delete",
			},
		},
		//	Plugin specific topics
		{
			Topic: "system.download",
			Config: map[string]string{
				// 12 hours
				"retention.ms":   "43200000",
				"segment.bytes":  "1073741824",
				"cleanup.policy": "delete",
			},
		},
		{
			Topic: "system.retrohunt",
		},
		{
			Topic: "system.report",
			Config: map[string]string{
				// 4 weeks
				"retention.ms":   "2419200000",
				"segment.bytes":  "1073741824",
				"cleanup.policy": "delete",
			},
		},
		{
			Topic: "system.scrape",
			Config: map[string]string{
				// 4 weeks
				"retention.ms":   "2419200000",
				"segment.bytes":  "1073741824",
				"cleanup.policy": "delete",
			},
		},
	}

}
