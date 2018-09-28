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
package com.flipkart.priority.kafka.client.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serializer;
import com.flipkart.priority.kafka.client.ClientConfigs;
import com.flipkart.priority.kafka.client.ClientUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Kafka priority producer implementation to produce across priority levels.
 * <br><br>
 * For every priority level i (0 &le; i &lt; $max.priority), broker must have kafka
 * topic XYZ-i and message produce on priority i will be sent to XYZ-i.<br>
 * Note: When a method parameter specifies topic name, it is the logical name ("XYZ") backed by
 * several kafka topics in the broker ("XYZ-0", "XYZ-1", ...). If the method parameter or return
 * object specifies any kafka specific class such as {@code TopicPartition}, then it is not the
 * logical name and is the actual kafka entity / data structure.
 */
public class PriorityKafkaProducer<K, V> extends AbstractPriorityKafkaProducer<K, V> {

    private int maxPriority;
    private KafkaProducer<K, V> producer;

    public PriorityKafkaProducer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    public PriorityKafkaProducer(Map<String, Object> configs,
                                 Serializer<K> keyDeserializer,
                                 Serializer<V> valueDeserializer) {
        Properties properties = new Properties();
        for (Map.Entry<String, Object> entry : configs.entrySet()) {
            properties.setProperty(entry.getKey(),
                    (entry.getValue() == null) ?  null : entry.getValue().toString());
        }
        initialize(properties, keyDeserializer, valueDeserializer);
    }

    public PriorityKafkaProducer(Properties properties) {
        this(properties, null, null);
    }

    public PriorityKafkaProducer(Properties properties,
                                 Serializer<K> keyDeserializer,
                                 Serializer<V> valueDeserializer) {
        initialize(properties, keyDeserializer, valueDeserializer);
    }

    PriorityKafkaProducer(int maxPriority, KafkaProducer<K, V> producer) {
        this.maxPriority = maxPriority;
        this.producer = producer;
    }

    private void initialize(Properties properties,
                            Serializer<K> keyDeserializer,
                            Serializer<V> valueDeserializer) {
        if (!properties.containsKey(ClientConfigs.MAX_PRIORITY_CONFIG)) {
            throw new IllegalArgumentException("Missing config " + ClientConfigs.MAX_PRIORITY_CONFIG);
        }
        this.maxPriority = Integer.parseInt(properties.getProperty(ClientConfigs.MAX_PRIORITY_CONFIG));
        this.producer = new KafkaProducer<K, V>(properties, keyDeserializer, valueDeserializer);
    }

    @Override
    public int getMaxPriority() {
        return maxPriority;
    }

    private ProducerRecord<K, V> getPriorityRecord(int priority, ProducerRecord<K, V> record) {
        if (priority < 0 || priority >= maxPriority) {
            throw new IllegalArgumentException("Priority param must be in the range [0, "
                    + (maxPriority - 1) + "]");
        }
        String priorityTopic = ClientUtils.getPriorityTopic(record.topic(), priority);
        return new ProducerRecord<K, V>(priorityTopic, record.partition(), record.timestamp(),
                record.key(), record.value());
    }
    @Override
    public Future<RecordMetadata> send(int priority, ProducerRecord<K, V> record) {
        return producer.send(getPriorityRecord(priority, record));
    }

    @Override
    public Future<RecordMetadata> send(int priority, ProducerRecord<K, V> record, Callback callback) {
        return producer.send(getPriorityRecord(priority, record), callback);
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();
        for (int i = 0; i < maxPriority; ++i) {
            partitions.addAll(producer.partitionsFor(ClientUtils.getPriorityTopic(topic, i)));
        }
        return partitions;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return producer.metrics();
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        producer.close(timeout, unit);
    }
}
