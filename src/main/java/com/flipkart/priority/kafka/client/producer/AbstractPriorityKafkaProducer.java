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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import com.flipkart.priority.kafka.client.ClientConfigs;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Abstract producer implementation for priority producers.
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
 *     <li>
 *         Exposes methods {@link #send(int, ProducerRecord)} and {@link #send(int, ProducerRecord, Callback)}
 *         to produce messages to a specific priority level topic. If {@link #send(ProducerRecord)} or
 *         {@link #send(ProducerRecord, Callback)} are used, then messages are defaulted to lowest
 *         priority level 0.
 *     </li>
 * </ul>
 */
public abstract class AbstractPriorityKafkaProducer<K, V> implements Producer<K, V> {

    public abstract int getMaxPriority();

    public abstract Future<RecordMetadata> send(int priority, ProducerRecord<K, V> record);

    public abstract Future<RecordMetadata> send(int priority, ProducerRecord<K, V> record, Callback callback);

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(0, record);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        return send(0, record, callback);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }
}
