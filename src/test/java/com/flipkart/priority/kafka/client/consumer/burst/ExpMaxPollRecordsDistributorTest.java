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

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ExpMaxPollRecordsDistributorTest {

    @Test
    public void testDistributionCase1() {
        Map<Integer, Integer> expected = new HashMap<Integer, Integer>() {{
            put(2, 29);
            put(1, 14);
            put(0, 7);
        }};
        Map<Integer, Integer> actual = ExpMaxPollRecordsDistributor.instance().distribution(3, 50);
        assertEquals(expected, actual);
    }

    @Test
    public void testDistributionCase2() {
        Map<Integer, Integer> expected = new HashMap<Integer, Integer>() {{
            put(2, 7);
            put(1, 2);
            put(0, 1);
        }};
        Map<Integer, Integer> actual = ExpMaxPollRecordsDistributor.instance().distribution(3, 10);
        assertEquals(expected, actual);
    }

    @Test
    public void testDistributionCase3() {
        Map<Integer, Integer> expected = new HashMap<Integer, Integer>() {{
            put(2, 4);
            put(1, 2);
            put(0, 1);
        }};
        Map<Integer, Integer> actual = ExpMaxPollRecordsDistributor.instance().distribution(3, 7);
        assertEquals(expected, actual);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDistributionInvalidCase() {
        ExpMaxPollRecordsDistributor.instance().distribution(3, 2);
    }
}
