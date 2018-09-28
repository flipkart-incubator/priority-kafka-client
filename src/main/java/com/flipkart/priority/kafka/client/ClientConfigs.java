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
package com.flipkart.priority.kafka.client;

public class ClientConfigs {

    // Common
    public static final String MAX_PRIORITY_CONFIG = "max.priority";

    // Consumer
    public static final String MAX_POLL_HISTORY_WINDOW_SIZE_CONFIG = "max.poll.history.window.size";
    public static final String MIN_POLL_WINDOW_MAXOUT_THRESHOLD_CONFIG = "min.poll.window.maxout.threshold";

    public static final int DEFAULT_MAX_POLL_HISTORY_WINDOW_SIZE = 6;
    public static final int DEFAULT_MIN_POLL_WINDOW_MAXOUT_SIZE = 4;
}
