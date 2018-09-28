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
 */package com.flipkart.priority.kafka.client;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClientUtilsTest {

    @Test
    public void testGetPriorityTopic() {
        assertEquals("XYZ-9",
                ClientUtils.getPriorityTopic("XYZ", 9));
    }

    @Test
    public void testGetPriorityConsumerGroup() {
        assertEquals("ABC-4",
                ClientUtils.getPriorityConsumerGroup("ABC", 4));
    }

    @Test
    public void testGetPriority() {
        assertEquals(6,
                ClientUtils.getPriority(ClientUtils.getPriorityTopic("XYZ", 6)));
    }
}
