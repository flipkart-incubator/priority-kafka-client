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

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;

/**
 * Provides distribution logic of maxPollRecords across priorities.
 * <br><br>
 * This is in the context of varying processing rates across priorities.
 * {@link CapacityBurstPriorityKafkaConsumer} uses {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG}
 * as the processing capacity parameter for tuning processing rates across priorities.
 */
public interface MaxPollRecordsDistributor {

    /**
     * Distribute maxPollRecords across all priorities (upto maxPriority {exclusive}).
     *
     * @param maxPriority Max priority levels
     * @param maxPollRecords Max poll records
     * @return Mapping of maxPollRecords for each priority from [0, maxPriority - 1].
     * These must add upto the input maxPollRecords.
     */
    Map<Integer, Integer> distribution(int maxPriority, int maxPollRecords);
}
