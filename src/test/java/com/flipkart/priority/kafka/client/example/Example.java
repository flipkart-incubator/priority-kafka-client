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
package com.flipkart.priority.kafka.client.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.flipkart.priority.kafka.client.ClientConfigs;
import com.flipkart.priority.kafka.client.ClientUtils;
import com.flipkart.priority.kafka.client.consumer.burst.CapacityBurstPriorityKafkaConsumer;
import com.flipkart.priority.kafka.client.producer.PriorityKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class Example {

    private static final Logger log = LoggerFactory.getLogger(Example.class);

    private static final String[] PAYLOADS = new String[3];
    private static final String TOPIC = "test-topic";
    private static final String GROUP = "test-group";
    private static final int MAX_PRIORITY = 3;
    private static final long START = System.currentTimeMillis();
    private static final AtomicInteger counter = new AtomicInteger(0);

    static {
        String PAYLOAD = "a";
        for (int i = 0; i < 9; ++i) {
            PAYLOAD = PAYLOAD + PAYLOAD;
        }
        PAYLOADS[0] = "0" + ClientUtils.PRIORITY_TOPIC_DELIMITER + PAYLOAD;
        PAYLOADS[1] = "1" + ClientUtils.PRIORITY_TOPIC_DELIMITER + PAYLOAD;
        PAYLOADS[2] = "2" + ClientUtils.PRIORITY_TOPIC_DELIMITER + PAYLOAD;
    }

    private static Producer<Integer, String> createNonPriorityProducer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        Producer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
        return producer;
    }

    private static org.apache.kafka.clients.consumer.Consumer<Integer, String> createNonPriorityConsumer(
            String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "10500000");
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "10500000");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "120000");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        org.apache.kafka.clients.consumer.Consumer<Integer, String> consumer
                = new KafkaConsumer<Integer, String>(props);
        return consumer;
    }

    private static void produceNonPriority() throws Exception {
        Producer<Integer, String> producer = createNonPriorityProducer();
        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (int i = 0; i < 100000; ++i) {
            for (int j = 0; j < MAX_PRIORITY; ++j) {
                ProducerRecord<Integer, String> record
                        = new ProducerRecord<Integer, String>(TOPIC, i, PAYLOADS[j]);
                futures.add(producer.send(record));
            }
        }
        for (Future<?> future : futures) {
            future.get();
        }
    }

    private static class Consumer implements Callable<Void> {

        private org.apache.kafka.clients.consumer.Consumer<Integer, String> consumer;
        private ExecutorService service;

        public Consumer(org.apache.kafka.clients.consumer.Consumer<Integer, String> consumer) {
            this.consumer = consumer;
            service = Executors.newFixedThreadPool(55);
        }

        private static class Task implements Callable<Void> {

            private ConsumerRecord<Integer, String> record;

            public Task(ConsumerRecord<Integer, String> record) {
                this.record = record;
            }

            @Override
            public Void call() throws Exception {
                Thread.sleep(1000);
                String priority = record.value().split(ClientUtils.PRIORITY_TOPIC_DELIMITER)[0];
                long timeTakenMs = System.currentTimeMillis() - START;
                log.info("After {} ms {} sec {} completed {}", timeTakenMs, (timeTakenMs / 1000),
                        priority, counter.incrementAndGet());
                return null;
            }
        }

        @Override
        public Void call() throws Exception {
            consumer.subscribe(Arrays.asList(TOPIC));
            while (true) {
                ConsumerRecords<Integer, String> records = consumer.poll(100);
                List<Future<?>> futures = new ArrayList<Future<?>>();
                for (TopicPartition partition : records.partitions()) {
                    for (ConsumerRecord<Integer, String> record : records.records(partition)) {
                        futures.add(service.submit(new Task(record)));
                    }
                }
                for (Future<?> future : futures) {
                    future.get();
                }
                consumer.commitSync();
            }
        }
    }

    private static void consumeNonPriority() throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(5);
        Future<?> future = null;
        for (int i = 0; i < 5; ++i) {
            future = service.submit(new Consumer(createNonPriorityConsumer(GROUP)));
        }
        future.get();
    }

    private static PriorityKafkaProducer<Integer, String> createPriorityProducer() {
        Properties props = new Properties();
        props.put(ClientConfigs.MAX_PRIORITY_CONFIG, String.valueOf(MAX_PRIORITY));
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        PriorityKafkaProducer<Integer, String> producer = new PriorityKafkaProducer<Integer, String>(props);
        return producer;
    }

    private static void producePriority() throws Exception {
        PriorityKafkaProducer<Integer, String> producer = createPriorityProducer();
        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (int i = 0; i < 100000; ++i) {
            for (int j = 0; j < MAX_PRIORITY; ++j) {
                ProducerRecord<Integer, String> record
                        = new ProducerRecord<Integer, String>(TOPIC, i, PAYLOADS[j]);
                futures.add(producer.send(j, record));
            }
        }
        for (Future<?> future : futures) {
            future.get();
        }
    }

    private static org.apache.kafka.clients.consumer.Consumer<Integer, String> createPriorityConsumer(
            String group) {
        Properties props = new Properties();
        props.put(ClientConfigs.MAX_PRIORITY_CONFIG, String.valueOf(MAX_PRIORITY));
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "10500000");
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "10500000");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "120000");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        org.apache.kafka.clients.consumer.Consumer<Integer, String> consumer
                = new CapacityBurstPriorityKafkaConsumer<Integer, String>(props);
        return consumer;
    }

    private static void consumePriority() throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(5);
        Future<?> future = null;
        for (int i = 0; i < 5; ++i) {
            future = service.submit(new Consumer(createPriorityConsumer(GROUP)));
        }
        future.get();
    }

    public static void main(String[] args) throws Exception {

        produceNonPriority();
        producePriority();
        consumeNonPriority();
        consumePriority();
    }
}
