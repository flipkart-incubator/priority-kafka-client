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

import static org.junit.Assert.*;

public class WindowTest {

    private Window window = new Window(6, 4, 50);

    @Test
    public void testWindowing() {
        assertFalse(window.isMaxedOutThresholdBreach());
        assertEquals(50, window.maxUnusedValue());
        // [10, 0, 0, 0, 0, 0]
        window.add(10);
        assertFalse(window.isMaxedOutThresholdBreach());
        assertEquals(40, window.maxUnusedValue());

        // [10, 10, 10, 10, 10, 10]
        for (int i = 1; i < 6; ++i) {
            window.add(10);
        }
        assertFalse(window.isMaxedOutThresholdBreach());
        assertEquals(40, window.maxUnusedValue());

        // [30, 10, 10, 10, 10, 10]
        window.add(30);
        assertFalse(window.isMaxedOutThresholdBreach());
        assertEquals(20, window.maxUnusedValue());

        // [50, 50, 50, 30, 10, 10]
        for (int i = 0; i < 3; ++i) {
            window.add(50);
        }
        assertFalse(window.isMaxedOutThresholdBreach());
        assertEquals(0, window.maxUnusedValue());

        // [50, 50, 50, 50, 30, 10]
        window.add(50);
        assertTrue(window.isMaxedOutThresholdBreach());
        assertEquals(0, window.maxUnusedValue());

        // [10, 20, 30, 50, 50, 50]
        window.add(30);
        window.add(20);
        window.add(10);
        assertFalse(window.isMaxedOutThresholdBreach());
        assertEquals(0, window.maxUnusedValue());
    }
}
