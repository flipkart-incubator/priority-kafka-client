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

import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Moving window implementation.
 */
class Window {

    /**
     * Current index
     */
    private int circularIdx;
    /**
     * Values in the window
     */
    private int[] values;
    /**
     * Captures sorted value along with freq
     */
    private SortedMap<Integer, Integer> sortedValues;
    /**
     * Threshold count of how many values are maxed out
     */
    private int maxedOutCountThreshold;
    /**
     * Max limit value or maxed out value
     */
    private int valueLimit;
    /**
     * Number of values that are maxed out
     */
    private int maxedOutCount;

    Window(final int size, int maxedOutCountThreshold, int valueLimit) {
        this.circularIdx = -1;
        this.values = new int[size];
        this.sortedValues = new TreeMap<Integer, Integer>() {{
            put(0, size);
        }};
        this.maxedOutCountThreshold = maxedOutCountThreshold;
        this.valueLimit = valueLimit;
        this.maxedOutCount = 0;
    }

    /**
     * Add a value to the circular buffer.
     */
    void add(int value) {
        int nextIdx = (circularIdx + 1) % values.length;

        // Update maxedOutCount
        if (value >= valueLimit && values[nextIdx] < valueLimit) {
            ++maxedOutCount;
        } else if (value < valueLimit && values[nextIdx] >= valueLimit) {
            --maxedOutCount;
        }

        // Update sortedValues
        sortedValues.put(values[nextIdx], sortedValues.get(values[nextIdx]) - 1);
        if (sortedValues.get(values[nextIdx]) == 0) {
            sortedValues.remove(values[nextIdx]);
        }
        if (!sortedValues.containsKey(value)) {
            sortedValues.put(value, 0);
        }
        sortedValues.put(value, sortedValues.get(value) + 1);

        values[nextIdx] = value;
        circularIdx = nextIdx;
    }

    /**
     * Check if {@code maxedOutCount} &ge; {@code maxedOutCountThreshold}.
     */
    boolean isMaxedOutThresholdBreach() {
        return maxedOutCount >= maxedOutCountThreshold;
    }

    /**
     * Return max unused value so far w.r.t. {@code valueLimit}.
     */
    int maxUnusedValue() {
        return Math.max(0, valueLimit - sortedValues.lastKey());
    }
}