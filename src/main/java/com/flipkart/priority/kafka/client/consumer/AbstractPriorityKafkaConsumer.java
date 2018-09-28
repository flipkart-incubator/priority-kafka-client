/*
 * Copyright 2018 Flipkart Internet, pvt ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.priority.kafka.client.consumer;

import com.flipkart.priority.kafka.client.ClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Abstract consumer implementation for priority consumers.
 * <br><br>
 * Implementations should follow the below instructions:
 * <ul>
 *     <li>
 *         Implementations should whitelist functionality and by default no operation is allowed.
 *     </li>
 *     <li>
 *         When we discuss about priority topic XYZ, here XYZ is the logical name.
 *         For every logical priority topic XYZ one must define max supported priority level via
 *         the config {@link ClientConfigs#MAX_PRIORITY_CONFIG} property.
 *     </li>
 *     <li>
 *         Priorities are ordered as follows: <code>0 &lt; 1 &lt; 2 ... &lt; ${max.priority - 1}</code>.
 *     </li>
 * </ul>
 */
public abstract class AbstractPriorityKafkaConsumer<K, V> implements Consumer<K, V> {

    public abstract int getMaxPriority();

    @Override
    public Set<TopicPartition> assignment() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> subscription() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subscribe(Collection<String> topics) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unsubscribe() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitSync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position(TopicPartition partition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<TopicPartition> paused() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void wakeup() {
        throw new UnsupportedOperationException();
    }
}
