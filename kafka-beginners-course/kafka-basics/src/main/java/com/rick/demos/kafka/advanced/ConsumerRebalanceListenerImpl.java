package com.rick.demos.kafka.advanced;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * @Author: Rick
 * @Date: 2023/10/6 16:05
 */
public class ConsumerRebalanceListenerImpl implements ConsumerRebalanceListener {
    private static final Logger log = LoggerFactory.getLogger(ConsumerRebalanceListenerImpl.class);

    private KafkaConsumer<String, String> consumer;
    private Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();

    public ConsumerRebalanceListenerImpl(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    public void addOffsetToTrack(String topic, int partition, long offset) {
        currentOffset.put(
                new TopicPartition(topic, partition),
                new OffsetAndMetadata(offset + 1, null));

    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("onPartitionsRevoked callback triggered");
        log.info("Committing offsets: " + currentOffset);

        consumer.commitSync(currentOffset);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("onPartitionsAssigned callback triggered");
    }

    // this is used when we shut down our consumer gracefully
    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffset(){
        return currentOffset;
    }
}
