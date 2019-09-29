package kafka

import (
	"fmt"
	"sort"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

// TopicDetail is all information we get when listing Kafka topics
type TopicDetail struct {
	TopicName         string `json:"topicName"`
	IsInternal        bool   `json:"isInternal"`
	PartitionCount    int    `json:"partitionCount"`
	ReplicationFactor int    `json:"replicationFactor"`
	CleanupPolicy     string `json:"cleanupPolicy"`
}

// ListTopics returns a List of all topics in a kafka cluster.
// Each topic entry contains details like ReplicationFactor, Cleanup Policy
func (s *Service) ListTopics() ([]*TopicDetail, error) {
	// 1. Get a random broker and connect to it
	broker, err := s.findAnyBroker()
	if err != nil {
		return nil, err
	}
	err = broker.Open(s.Client.Config())
	if err != nil && err != sarama.ErrAlreadyConnected {
		s.Logger.Warn("opening the broker connection failed", zap.Error(err))
	}

	// 2. Refresh metadata to ensure we get an up to date list of available topics
	metadata, err := broker.GetMetadata(&sarama.MetadataRequest{})
	if err != nil {
		return nil, err
	}

	topicsByName := make(map[string]*TopicDetail, len(metadata.Topics))

	// 3. Create config resources request objects for all topics
	describeCfgResources := make([]*sarama.ConfigResource, len(metadata.Topics))
	for i, topic := range metadata.Topics {
		if topic.Err != sarama.ErrNoError {
			s.Logger.Error("failed to get topic metadata while listing topics",
				zap.String("topic_name", topic.Name),
				zap.Error(err))
			return nil, topic.Err
		}

		topicsByName[topic.Name] = &TopicDetail{
			TopicName:         topic.Name,
			IsInternal:        topic.IsInternal,
			PartitionCount:    len(topic.Partitions),
			ReplicationFactor: len(topic.Partitions[0].Replicas),
		}

		topicResource := &sarama.ConfigResource{
			Type:        sarama.TopicResource,
			Name:        topic.Name,
			ConfigNames: []string{"cleanup.policy"},
		}
		describeCfgResources[i] = topicResource
	}

	// 4. Get topics' config entries
	describeConfigsReq := &sarama.DescribeConfigsRequest{
		Resources: describeCfgResources,
	}
	describeConfigsResp, err := broker.DescribeConfigs(describeConfigsReq)
	if err != nil {
		return nil, err
	}
	for _, resource := range describeConfigsResp.Resources {
		topicName := resource.Name
		if resource.ErrorMsg != "" {
			return nil, fmt.Errorf(resource.ErrorMsg)
		}

		for _, config := range resource.Configs {
			if config.Name != "cleanup.policy" {
				continue
			}
			topicsByName[topicName].CleanupPolicy = config.Value
		}
	}

	// 5. Return map as array which is sorted by topic name
	sortedKeys := make([]string, 0, len(topicsByName))
	for k := range topicsByName {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	response := make([]*TopicDetail, len(sortedKeys))
	for i, key := range sortedKeys {
		response[i] = topicsByName[key]
	}

	return response, nil
}

// TODO: Currently unused
func (s *Service) highWaterMarksBulk(topicName string, partitionIDs []int32) (map[int32]*sarama.OffsetResponseBlock, error) {
	broker, err := s.findAnyBroker()
	if err != nil {
		return nil, err
	}

	req := &sarama.OffsetRequest{}
	for _, partitionID := range partitionIDs {
		req.AddBlock(topicName, partitionID, sarama.OffsetNewest, 1)
	}

	offsets, err := broker.GetAvailableOffsets(req)
	if err != nil {
		return nil, err
	}
	block, exists := offsets.Blocks[topicName]
	if !exists {
		return nil, fmt.Errorf("The requested topic '%v' does not exist", topicName)
	}

	return block, nil
}