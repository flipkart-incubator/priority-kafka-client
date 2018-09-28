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
package com.flipkart.priority.kafka.client.consumer.burst;

import com.flipkart.priority.kafka.client.producer.PriorityKafkaProducer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.Fetcher;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import com.flipkart.priority.kafka.client.ClientConfigs;
import com.flipkart.priority.kafka.client.ClientUtils;
import com.flipkart.priority.kafka.client.consumer.AbstractPriorityKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

/**
 * Kafka priority consumer implementation that regulates capacity (record processing rate) across priority levels.
 * <br><br>
 * This implementation works as follows:
 * <ul>
 *     <li>
 *         When we discuss about priority topic XYZ and consumer group ABC, here XYZ and ABC are the
 *         logical names. For every logical priority topic XYZ one must define max supported priority
 *         level via the config {@link ClientConfigs#MAX_PRIORITY_CONFIG} property.
 *         This property is used as the capacity lever for tuning processing rate across priorities.
 *         Every object of the class maintains {@link KafkaConsumer} instance for every priority level
 *         topic [0, ${max.priority - 1}].
 *         <ul>
 *             <li>
 *                  For every priority level i (0 &le; i &lt; $max.priority), broker must have kafka
 *                  topic XYZ-i and priority topic consumer will use group ABC-i for consumption.
 *                  This implementation mandates providing {@code group.id}.
 *             </li>
 *             <li>
 *                 This works in tandem with {@link PriorityKafkaProducer}.
 *             </li>
 *             <li>
 *                 {@code max.poll.records} property is split across priority topic consumers based on
 *                 {@code maxPollRecordsDistributor} - defaulted to {@link ExpMaxPollRecordsDistributor}.
 *                 This implementation mandates providing {@code max.poll.records}.
 *             </li>
 *             <li>
 *                  Rest of the {@link KafkaConsumer} configs are passed as is to each of the priority
 *                  topic consumers. Care has to be taken when defining {@code max.partition.fetch.bytes},
 *                  {@code fetch.max.bytes} and {@code max.poll.interval.ms} as these values will be used
 *                  as is across all the priority topic consumers.
 *             </li>
 *         </ul>
 *     </li>
 *     <li>
 *          Works on the idea of distributing {@code max.poll.records} property across each of the
 *          priority topic consumers as their reserved capacity. Records are fetched sequentially from
 *          all priority level topics consumers which are configured with distributed {@code max.poll.records}
 *          values. The distribution must reserve higher capacity or processing rate to higher priorities.
 *     </li>
 *     <li>
 *         Caution 1 - if we have skewed partitions in priority level topics e.g. 10K records in a priority
 *         2 partition, 100 records in a priority 1 partition, 10 records in a priority 0 partition that
 *         are assigned to different consumer threads, then the implementation will not synchronize across
 *         such consumers to regulate capacity and hence will fail to honour priority. So the producers
 *         must ensure there are no skewed partitions (e.g. using round-robin - this "may" imply there
 *         is no message ordering assumptions and the consumer may choose to process records in parallel
 *         by separating out fetching and processing concerns).
 *     </li>
 *     <li>
 *         Caution 2 - If we have empty partitions in priority level topics e.g. no pending records in
 *         assigned priority 2 and 1 partitions, 10K records in priority 0 partition that are assigned to
 *         the same consumer thread, then we want priority 0 topic partition consumer to burst its capacity
 *         to {@code max.poll.records} and not restrict itself to its reserved capacity based on
 *         {@code maxPollRecordsDistributor} else the overall capacity will be under utilized.
 *     </li>
 *     <li>
 *         This implementation will try to address cautions explained above. Every consumer object will have
 *         individual priority level topic consumers, with each priority level consumer having reserved capacity
 *         based on {@code maxPollRecordsDistributor}. Each of the priority level topic consumer will try to
 *         burst into other priority level topic consumer's capacity in the group provided all the below are true:
 *         <ul>
 *             <li>
 *                 It is eligible to burst - This is iff in the last
 *                 {@link ClientConfigs#MAX_POLL_HISTORY_WINDOW_SIZE_CONFIG} attempts of
 *                 {@code KafkaConsumer#poll(long)} atleast {@link ClientConfigs#MIN_POLL_WINDOW_MAXOUT_THRESHOLD_CONFIG}
 *                 times it has received number of records equal to assigned {@code max.poll.records}
 *                 which was distributed based on {@code maxPollRecordsDistributor}. This is an indication
 *                 that the partition has more incoming records to be processed.
 *             </li>
 *             <li>
 *                 Higher priority level is not eligible to burst - There is no higher priority level topic
 *                 consumer that is eligible to burst based on above logic. Basically give way to higher priorities.
 *             </li>
 *         </ul>
 *         If the above are true, then the priority level topic consumer will burst into all other priority
 *         level topic consumer capacities. The amount of burst per priority level topic consumer is equal
 *         to the least un-used capacity in the last {@link ClientConfigs#MAX_POLL_HISTORY_WINDOW_SIZE_CONFIG}
 *         attempts of {@code KafkaConsumer#poll(long)}.
 *     </li>
 *     <li>
 *         For example say we have<br>
 *         {@link ClientConfigs#MAX_PRIORITY_CONFIG} = 3;<br>
 *         {@code max.poll.records} = 50;<br>
 *         {@code maxPollRecordsDistributor} = {@link ExpMaxPollRecordsDistributor#instance()};<br>
 *         {@link ClientConfigs#MAX_POLL_HISTORY_WINDOW_SIZE_CONFIG} = 6;<br>
 *         {@link ClientConfigs#MIN_POLL_WINDOW_MAXOUT_THRESHOLD_CONFIG} = 4;<br>
 *         Capacity distribution of {@code max.poll.records} property is {2=29, 1=14, 0=7}.
 *         <ul>
 *             <li>
 *                 Case 1: FIFO view of poll window is<br>
 *                 2 - [29, 29, 29, 29, 29, 29];<br>
 *                 1 - [14, 14, 14, 14, 14, 14];<br>
 *                 0 - [7, 7, 7, 7, 7, 7]<br>
 *                 In this case every priority level topic consumer will retain its capacity and not burst as
 *                 everyone is eligible to burst but no one is ready to give away reserved capacity.
 *             </li>
 *             <li>
 *                 Case 2: FIFO view of poll window is<br>
 *                 2 - [29, 29, 29, 29, 29, 29];<br>
 *                 1 - [14, 14, 14, 11, 10, 9];<br>
 *                 0 - [7, 7, 7, 7, 7, 7]<br>
 *                 In this case every priority level topic consumer will retain its capacity and not burst as
 *                 everyone is eligible to burst but no one is ready to give away reserved capacity.
 *             </li>
 *             <li>
 *                 Case 3: FIFO view of poll window is<br>
 *                 2 - [29, 29, 29, 29, 29, 29];<br>
 *                 1 - [10, 10, 7, 10, 9, 0];<br>
 *                 0 - [7, 7, 7, 7, 7, 7]<br>
 *                 In this case priority level 2 topic consumer will burst into priority level 1 topic
 *                 consumer's capacity and steal (14 - 10 = 4), hence increasing its capacity or
 *                 {@code max.poll.records} property to (29 + 4 = 33).
 *             </li>
 *             <li>
 *                 Case 2: FIFO view of poll window is<br>
 *                 2 - [20, 25, 25, 20, 15, 10];<br>
 *                 1 - [10, 10, 7, 10, 9, 0];<br>
 *                 0 - [7, 7, 7, 7, 7, 7]<br>
 *                 In this case priority level 0 topic consumer will burst into priority level 2 and 1
 *                 topic consumer capacities and steal (29 - 25 = 4 and 14 - 10 = 4), hence increasing
 *                 its capacity or {@code max.poll.records} property to (7 + 8 = 15).
 *             </li>
 *         </ul>
 *     </li>
 * </ul>
 * Note: When a method parameter specifies topic name, it is the logical name ("XYZ") backed by
 * several kafka topics in the broker ("XYZ-0", "XYZ-1", ...). If the method parameter or return
 * object specifies any kafka specific class such as {@code TopicPartition}, then it is not the
 * logical name and is the actual kafka entity / data structure.
 */
public class CapacityBurstPriorityKafkaConsumer<K, V> extends AbstractPriorityKafkaConsumer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(CapacityBurstPriorityKafkaConsumer.class);
    private static final String FETCHER_FIELD = "fetcher";
    private static final String MAX_POLL_RECORDS_FIELD = "maxPollRecords";

    private int maxPriority;
    private Map<Integer, Integer> maxPollRecordDistribution;
    private Map<Integer, KafkaConsumer<K, V>> consumers;
    private Map<Integer, Window> consumerPollWindowHistory;

    public CapacityBurstPriorityKafkaConsumer(Map<String, Object> configs) {
        this(ExpMaxPollRecordsDistributor.instance(), configs, null, null);
    }

    public CapacityBurstPriorityKafkaConsumer(MaxPollRecordsDistributor maxPollRecordsDistributor,
                                              Map<String, Object> configs,
                                              Deserializer<K> keyDeserializer,
                                              Deserializer<V> valueDeserializer) {
        Properties properties = new Properties();
        for (Map.Entry<String, Object> entry : configs.entrySet()) {
            properties.setProperty(entry.getKey(),
                    (entry.getValue() == null) ?  null : entry.getValue().toString());
        }
        initialize(maxPollRecordsDistributor, properties, keyDeserializer, valueDeserializer);
    }

    public CapacityBurstPriorityKafkaConsumer(Properties properties) {
        this(ExpMaxPollRecordsDistributor.instance(), properties, null, null);
    }

    public CapacityBurstPriorityKafkaConsumer(MaxPollRecordsDistributor maxPollRecordsDistributor,
                                              Properties properties,
                                              Deserializer<K> keyDeserializer,
                                              Deserializer<V> valueDeserializer) {
        initialize(maxPollRecordsDistributor, properties, keyDeserializer, valueDeserializer);
    }

    CapacityBurstPriorityKafkaConsumer(int maxPriority,
                                       int maxPollRecords,
                                       Map<Integer, KafkaConsumer<K, V>> consumers,
                                       Map<Integer, Window> consumerPollWindowHistory) {
        this.maxPriority = maxPriority;
        this.maxPollRecordDistribution = ExpMaxPollRecordsDistributor.instance()
                .distribution(maxPriority, maxPollRecords);
        this.consumerPollWindowHistory = consumerPollWindowHistory;
        this.consumers = consumers;
    }

    private void initialize(MaxPollRecordsDistributor maxPollRecordsDistributor,
                            Properties properties,
                            Deserializer<K> keyDeserializer,
                            Deserializer<V> valueDeserializer) {
        if (!properties.containsKey(ClientConfigs.MAX_PRIORITY_CONFIG)) {
            throw new IllegalArgumentException("Missing config " + ClientConfigs.MAX_PRIORITY_CONFIG);
        }
        this.maxPriority = Integer.parseInt(properties.getProperty(ClientConfigs.MAX_PRIORITY_CONFIG));

        if (!properties.containsKey(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)) {
            throw new IllegalArgumentException("Missing config " + ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        }
        int maxPollRecords = Integer.parseInt(properties.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));

        if (!properties.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            throw new IllegalArgumentException("Missing config " + ConsumerConfig.GROUP_ID_CONFIG);
        }
        String groupId = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);

        int maxPollHistoryWindowSize = ClientConfigs.DEFAULT_MAX_POLL_HISTORY_WINDOW_SIZE;
        if (properties.containsKey(ClientConfigs.MAX_POLL_HISTORY_WINDOW_SIZE_CONFIG)) {
            maxPollHistoryWindowSize = Integer.parseInt(
                    properties.getProperty(ClientConfigs.MAX_POLL_HISTORY_WINDOW_SIZE_CONFIG));
        }

        int minPollWindowMaxoutSize = ClientConfigs.DEFAULT_MIN_POLL_WINDOW_MAXOUT_SIZE;
        if (properties.containsKey(ClientConfigs.MIN_POLL_WINDOW_MAXOUT_THRESHOLD_CONFIG)) {
            minPollWindowMaxoutSize = Integer.parseInt(
                    properties.getProperty(ClientConfigs.MIN_POLL_WINDOW_MAXOUT_THRESHOLD_CONFIG));
        }

        this.maxPollRecordDistribution = maxPollRecordsDistributor.distribution(maxPriority,
                maxPollRecords);
        log.info("Using {} distribution {}", ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                this.maxPollRecordDistribution);
        this.consumers = new HashMap<Integer, KafkaConsumer<K, V>>(maxPriority);
        this.consumerPollWindowHistory = new HashMap<Integer, Window>(maxPriority);
        for (int i = 0; i < maxPriority; ++i) {
            int consumerMaxPollRecords = this.maxPollRecordDistribution.get(i);
            String consumerGroupId = ClientUtils.getPriorityConsumerGroup(groupId, i);
            properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerMaxPollRecords);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            this.consumers.put(i, new KafkaConsumer<K, V>(properties, keyDeserializer, valueDeserializer));
            this.consumerPollWindowHistory.put(i,
                    new Window(maxPollHistoryWindowSize, minPollWindowMaxoutSize, consumerMaxPollRecords));
        }
    }

    @Override
    public int getMaxPriority() {
        return maxPriority;
    }

    @Override
    public Set<TopicPartition> assignment() {
        Set<TopicPartition> assignments = new HashSet<TopicPartition>();
        for (KafkaConsumer<K, V> consumer : consumers.values()) {
            assignments.addAll(consumer.assignment());
        }
        return assignments;
    }

    @Override
    public Set<String> subscription() {
        Set<String> subscriptions = new HashSet<String>();
        for (KafkaConsumer<K, V> consumer : consumers.values()) {
            subscriptions.addAll(consumer.subscription());
        }
        return subscriptions;
    }

    @Override
    public void subscribe(Collection<String> topics) {
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        for (int i = 0; i < maxPriority; ++i) {
            Collection<String> priorityTopics = new ArrayList<String>(topics.size());
            for (String topic : topics) {
                priorityTopics.add(ClientUtils.getPriorityTopic(topic, i));
            }
            consumers.get(i).subscribe(priorityTopics, callback);
        }
    }

    private Map<Integer, Collection<TopicPartition>> splitPriorityPartitions(Collection<TopicPartition> partitions) {
        Map<Integer, Collection<TopicPartition>> priorityPartitions
                = new HashMap<Integer, Collection<TopicPartition>>();
        for (TopicPartition partition : partitions) {
            int priority = ClientUtils.getPriority(partition.topic());
            if (!priorityPartitions.containsKey(priority)) {
                priorityPartitions.put(priority, new ArrayList<TopicPartition>());
            }
            priorityPartitions.get(priority).add(partition);
        }
        return priorityPartitions;
    }

    @Override
    public void unsubscribe() {
        for (KafkaConsumer<K, V> consumer : consumers.values()) {
            consumer.unsubscribe();
        }
    }

    @Override
    public void commitSync() {
        for (KafkaConsumer<K, V> consumer : consumers.values()) {
            consumer.commitSync();
        }
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        Map<Integer, Map<TopicPartition, OffsetAndMetadata>> priorityOffsets
                = new HashMap<Integer, Map<TopicPartition, OffsetAndMetadata>>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            TopicPartition partition = entry.getKey();
            int priority = ClientUtils.getPriority(partition.topic());
            if (!priorityOffsets.containsKey(priority)) {
                priorityOffsets.put(priority, new HashMap<TopicPartition, OffsetAndMetadata>());
            }
            priorityOffsets.get(priority).put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Integer, Map<TopicPartition, OffsetAndMetadata>> entry : priorityOffsets.entrySet()) {
            consumers.get(entry.getKey()).commitSync(entry.getValue());
        }
    }

    @Override
    public void commitAsync() {
        for (KafkaConsumer<K, V> consumer : consumers.values()) {
            consumer.commitAsync();
        }
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        int priority = ClientUtils.getPriority(partition.topic());
        consumers.get(priority).seek(partition, offset);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        Map<Integer, Collection<TopicPartition>> priorityPartitions = splitPriorityPartitions(partitions);
        for (Map.Entry<Integer, Collection<TopicPartition>> entry : priorityPartitions.entrySet()) {
            consumers.get(entry.getKey()).seekToBeginning(entry.getValue());
        }
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        Map<Integer, Collection<TopicPartition>> priorityPartitions = splitPriorityPartitions(partitions);
        for (Map.Entry<Integer, Collection<TopicPartition>> entry : priorityPartitions.entrySet()) {
            consumers.get(entry.getKey()).seekToEnd(entry.getValue());
        }
    }

    @Override
    public long position(TopicPartition partition) {
        int priority = ClientUtils.getPriority(partition.topic());
        return consumers.get(priority).position(partition);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        int priority = ClientUtils.getPriority(partition.topic());
        return consumers.get(priority).committed(partition);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        Map<MetricName, Metric> metrics = new HashMap<MetricName, Metric>();
        for (KafkaConsumer<K, V> consumer : consumers.values()) {
            metrics.putAll(consumer.metrics());
        }
        return metrics;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();
        for (int i = 0; i < maxPriority; ++i) {
            partitions.addAll(consumers.get(i).partitionsFor(ClientUtils.getPriorityTopic(topic, i)));
        }
        return partitions;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        Map<String, List<PartitionInfo>> topics = new HashMap<String, List<PartitionInfo>>();
        for (KafkaConsumer<K, V> consumer : consumers.values()) {
            topics.putAll(consumer.listTopics());
        }
        return topics;
    }

    @Override
    public Set<TopicPartition> paused() {
        Set<TopicPartition> paused = new HashSet<TopicPartition>();
        for (KafkaConsumer<K, V> consumer : consumers.values()) {
            paused.addAll(consumer.paused());
        }
        return paused;
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        Map<Integer, Collection<TopicPartition>> priorityPartitions = splitPriorityPartitions(partitions);
        for (Map.Entry<Integer, Collection<TopicPartition>> entry : priorityPartitions.entrySet()) {
            consumers.get(entry.getKey()).pause(entry.getValue());
        }
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        Map<Integer, Collection<TopicPartition>> priorityPartitions = splitPriorityPartitions(partitions);
        for (Map.Entry<Integer, Collection<TopicPartition>> entry : priorityPartitions.entrySet()) {
            consumers.get(entry.getKey()).resume(entry.getValue());
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
            Map<TopicPartition, Long> timestampsToSearch) {
        Map<Integer, Map<TopicPartition, Long>> priorityTimestampsToSearch
                = new HashMap<Integer, Map<TopicPartition, Long>>();
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            int priority = ClientUtils.getPriority(entry.getKey().topic());
            if (!priorityTimestampsToSearch.containsKey(priority)) {
                priorityTimestampsToSearch.put(priority, new HashMap<TopicPartition, Long>());
            }
            priorityTimestampsToSearch.get(priority).put(entry.getKey(), entry.getValue());
        }
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes
                = new HashMap<TopicPartition, OffsetAndTimestamp>();
        for (Map.Entry<Integer, Map<TopicPartition, Long>> entry : priorityTimestampsToSearch.entrySet()) {
            offsetsForTimes.putAll(consumers.get(entry.getKey()).offsetsForTimes(entry.getValue()));
        }
        return offsetsForTimes;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        Map<Integer, Collection<TopicPartition>> priorityPartitions = splitPriorityPartitions(partitions);
        Map<TopicPartition, Long> beginningOffsets = new HashMap<TopicPartition, Long>();
        for (Map.Entry<Integer, Collection<TopicPartition>> entry : priorityPartitions.entrySet()) {
            beginningOffsets.putAll(consumers.get(entry.getKey()).beginningOffsets(entry.getValue()));
        }
        return beginningOffsets;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        Map<Integer, Collection<TopicPartition>> priorityPartitions = splitPriorityPartitions(partitions);
        Map<TopicPartition, Long> endOffsets = new HashMap<TopicPartition, Long>();
        for (Map.Entry<Integer, Collection<TopicPartition>> entry : priorityPartitions.entrySet()) {
            endOffsets.putAll(consumers.get(entry.getKey()).endOffsets(entry.getValue()));
        }
        return endOffsets;
    }

    @Override
    public void close() {
        for (KafkaConsumer<K, V> consumer : consumers.values()) {
            consumer.close();
        }
    }

    @Override
    public void wakeup() {
        for (KafkaConsumer<K, V> consumer : consumers.values()) {
            consumer.wakeup();
        }
    }

    /**
     * Check if given priority is eligible to burst.
     * <br><br>
     * This is iff {@link Window#isMaxedOutThresholdBreach()} is true.
     */
    private boolean isEligibleToBurst(int priority) {
        Window window = consumerPollWindowHistory.get(priority);
        return window.isMaxedOutThresholdBreach();
    }

    /**
     * Burst capacity for given priority level.
     * <br><br>
     * It will try to burst {@link Window#maxUnusedValue()} of capacity from all other priorities.
     */
    private int burstCapacity(int priority) {
        int burst = 0;
        for (int i = 0; i < maxPriority; ++i) {
            if (i != priority) {
                Window window = consumerPollWindowHistory.get(i);
                burst += window.maxUnusedValue();
            }
        }
        return burst;
    }

    // TODO: Figure a way to avoid reflection ugliness
    void updateMaxPollRecords(KafkaConsumer<K, V> consumer, int maxPollRecords) {
        try {
            Field fetcherField = org.apache.kafka.clients.consumer.KafkaConsumer.class.getDeclaredField(FETCHER_FIELD);
            fetcherField.setAccessible(true);
            Fetcher fetcher = (Fetcher) fetcherField.get(consumer);
            Field maxPollRecordsField = Fetcher.class.getDeclaredField(MAX_POLL_RECORDS_FIELD);
            maxPollRecordsField.setAccessible(true);
            maxPollRecordsField.set(fetcher, maxPollRecords);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public ConsumerRecords<K, V> poll(long pollTimeoutMs) {
        int priorityBurst = -1;
        try {
            for (int i = maxPriority - 1; i >= 0; --i) {
                if (isEligibleToBurst(i)) {
                    int burstCapacity = burstCapacity(i);
                    if (burstCapacity > 0) {
                        priorityBurst = i;
                        int finalCapacity = burstCapacity + maxPollRecordDistribution.get(i);
                        log.info("Burst in capacity for priority {} to {}", priorityBurst, finalCapacity);
                        updateMaxPollRecords(consumers.get(priorityBurst), finalCapacity);
                    }
                    break;
                }
            }
            Map<TopicPartition, List<ConsumerRecord<K, V>>> consumerRecords
                    = new HashMap<TopicPartition, List<ConsumerRecord<K, V>>>();
            Map<Integer, Integer> pollCounts = new HashMap<Integer, Integer>();
            for (int i = maxPriority - 1; i >= 0; --i) {
                pollCounts.put(i, 0);
            }
            long start = System.currentTimeMillis();
            do {
                for (int i = maxPriority - 1; i >= 0; --i) {
                    ConsumerRecords<K, V> records = consumers.get(i).poll(0);
                    pollCounts.put(i, pollCounts.get(i) + records.count());
                    for (TopicPartition partition : records.partitions()) {
                        consumerRecords.put(partition, records.records(partition));
                    }
                }
            } while (consumerRecords.isEmpty() && System.currentTimeMillis() < (start + pollTimeoutMs));
            for (int i = maxPriority - 1; i >= 0; --i) {
                consumerPollWindowHistory.get(i).add(pollCounts.get(i));
            }
            return new ConsumerRecords<K, V>(consumerRecords);
        } finally {
            if (priorityBurst >=0) {
                updateMaxPollRecords(consumers.get(priorityBurst),
                        maxPollRecordDistribution.get(priorityBurst));
            }
        }
    }
}
