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

import java.util.HashMap;
import java.util.Map;

/**
 * Exponential distribution of maxPollRecords across priorities.
 * <br><br>
 * Sample distributions:
 * <ul>
 *     <li>
 *         For maxPollRecords = 50, maxPriority = 3 : {2=29, 1=14, 1=7}
 *     </li>
 *     <li>
 *         For maxPollRecords = 10, maxPriority = 3 : {2=7, 1=2, 0=1}
 *     </li>
 *     <li>
 *         For maxPollRecords = 7, maxPriority = 3 : {2=4, 1=2, 0=1}
 *     </li>
 *     <li>
 *         For maxPollRecords = 2, maxPriority = 3 : IllegalArgumentException
 *     </li>
 * </ul>
 */
public class ExpMaxPollRecordsDistributor implements MaxPollRecordsDistributor {

    private static final ExpMaxPollRecordsDistributor INSTANCE = new ExpMaxPollRecordsDistributor();

    private ExpMaxPollRecordsDistributor() {}

    public static ExpMaxPollRecordsDistributor instance() {
        return INSTANCE;
    }

    /**
     * Exponential distribution of maxPollRecords across all priorities (upto maxPriority).
     * <br><br>
     * Ensure input maxPollRecords is atleast <code>(1 &lt;&lt; maxPriority) - 1</code><br>
     * This class is fragile w.r.t. integer overflow, so ensure sane values are provided.
     *
     * @param maxPriority Max priority levels
     * @param maxPollRecords Max poll records
     * @return Mapping of maxPollRecords for each priority from [0, maxPriority].
     * These must add upto the input maxPollRecords.
     */
    @Override
    public Map<Integer, Integer> distribution(int maxPriority, int maxPollRecords) {
        int sum = (1 << maxPriority) - 1;
        if (maxPollRecords < sum) {
            throw new IllegalArgumentException("Ensure maxPollRecords "
                    + maxPollRecords + " is atleast " + sum);
        }
        Map<Integer, Integer> maxPollRecordsDistribution = new HashMap<Integer, Integer>(maxPriority);
        int left = maxPollRecords;
        for (int i = maxPriority - 1; i >= 0; --i) {
            int assign = (maxPollRecords * (1 << i)) / sum;
            maxPollRecordsDistribution.put(i, assign);
            left -= assign;
        }
        // Assign excess to priority {maxPriority - 1}
        maxPollRecordsDistribution.put(maxPriority - 1,
                maxPollRecordsDistribution.get(maxPriority - 1) + left);
        return maxPollRecordsDistribution;
    }
}
