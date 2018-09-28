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

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import com.flipkart.priority.kafka.client.ClientConfigs;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class CapacityBurstPriorityKafkaConsumerTest {

    private int maxPriority = 3;
    private int maxPollRecords = 50;
    private KafkaConsumer<Integer, String> consumer0;
    private KafkaConsumer<Integer, String> consumer1;
    private KafkaConsumer<Integer, String> consumer2;
    private Map<Integer, KafkaConsumer<Integer, String>> consumers;
    private CapacityBurstPriorityKafkaConsumer<Integer, String> priorityKafkaConsumer;

    @Before
    public void before() {
        consumer0 = mock(KafkaConsumer.class);
        consumer1 = mock(KafkaConsumer.class);
        consumer2 = mock(KafkaConsumer.class);
        consumers = new HashMap<Integer, KafkaConsumer<Integer, String>>() {{
            put(0, consumer0);
            put(1, consumer1);
            put(2, consumer2);
        }};
        priorityKafkaConsumer = new CapacityBurstPriorityKafkaConsumer<Integer, String>(
                maxPriority, maxPollRecords, consumers, null);
    }

    @After
    public void after() {
        verifyNoMoreInteractions(consumer0, consumer1, consumer2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingMaxPriorityConfig() {
        Map<String, Object> configs = new HashMap<String, Object>() {{
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
            put(ConsumerConfig.GROUP_ID_CONFIG, "ABC");
        }};
        new CapacityBurstPriorityKafkaConsumer<Integer, String>(configs);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingMaxPollRecordsConfig() {
        Properties properties = new Properties();
        properties.put(ClientConfigs.MAX_PRIORITY_CONFIG, 3);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "ABC");
        new CapacityBurstPriorityKafkaConsumer<Integer, String>(properties);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingGroupIdConfig() {
        Properties properties = new Properties();
        properties.put(ClientConfigs.MAX_PRIORITY_CONFIG, 3);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        new CapacityBurstPriorityKafkaConsumer<Integer, String>(properties);
    }

    @Test
    public void testGetMaxPriority() {
        assertEquals(maxPriority, priorityKafkaConsumer.getMaxPriority());
    }

    @Test
    public void testAssignment() {
        final TopicPartition partition0 = new TopicPartition("XYZ-0", 0);
        final TopicPartition partition1 = new TopicPartition("XYZ-1", 1);
        final TopicPartition partition2 = new TopicPartition("XYZ-2", 2);
        when(consumer0.assignment()).thenReturn(new HashSet<TopicPartition>() {{
            add(partition0);
        }});
        when(consumer1.assignment()).thenReturn(new HashSet<TopicPartition>() {{
            add(partition1);
        }});
        when(consumer2.assignment()).thenReturn(new HashSet<TopicPartition>() {{
            add(partition2);
        }});
        Set<TopicPartition> expected = new HashSet<TopicPartition>() {{
            add(partition0);
            add(partition1);
            add(partition2);
        }};
        assertEquals(expected, priorityKafkaConsumer.assignment());
        verify(consumer0).assignment();
        verify(consumer1).assignment();
        verify(consumer2).assignment();
    }

    @Test
    public void testSubscription() {
        final String subscription0 = "s0";
        final String subscription1 = "s1";
        final String subscription2 = "s2";
        when(consumer0.subscription()).thenReturn(new HashSet<String>() {{
            add(subscription0);
        }});
        when(consumer1.subscription()).thenReturn(new HashSet<String>() {{
            add(subscription1);
        }});
        when(consumer2.subscription()).thenReturn(new HashSet<String>() {{
            add(subscription2);
        }});
        Set<String> expected = new HashSet<String>() {{
            add(subscription0);
            add(subscription1);
            add(subscription2);
        }};
        assertEquals(expected, priorityKafkaConsumer.subscription());
        verify(consumer0).subscription();
        verify(consumer1).subscription();
        verify(consumer2).subscription();
    }

    @Test
    public void testSubscribe() {
        Collection<String> topics = Arrays.asList("WXY", "XYZ");
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Collection<String> topics = (Collection<String>) invocationOnMock.getArguments()[0];
                ConsumerRebalanceListener listener
                        = (ConsumerRebalanceListener) invocationOnMock.getArguments()[1];
                Collection<String> expectedTopics = Arrays.asList("WXY-0", "XYZ-0");
                assertEquals(expectedTopics, topics);
                assertTrue(listener instanceof NoOpConsumerRebalanceListener);
                return null;
            }
        }).when(consumer0).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Collection<String> topics = (Collection<String>) invocationOnMock.getArguments()[0];
                ConsumerRebalanceListener listener
                        = (ConsumerRebalanceListener) invocationOnMock.getArguments()[1];
                Collection<String> expectedTopics = Arrays.asList("WXY-1", "XYZ-1");
                assertEquals(expectedTopics, topics);
                assertTrue(listener instanceof NoOpConsumerRebalanceListener);
                return null;
            }
        }).when(consumer1).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Collection<String> topics = (Collection<String>) invocationOnMock.getArguments()[0];
                ConsumerRebalanceListener listener
                        = (ConsumerRebalanceListener) invocationOnMock.getArguments()[1];
                Collection<String> expectedTopics = Arrays.asList("WXY-2", "XYZ-2");
                assertEquals(expectedTopics, topics);
                assertTrue(listener instanceof NoOpConsumerRebalanceListener);
                return null;
            }
        }).when(consumer2).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
        priorityKafkaConsumer.subscribe(topics);
        verify(consumer0).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
        verify(consumer1).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
        verify(consumer2).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAssign() {
        final TopicPartition partition0 = new TopicPartition("XYZ-0", 0);
        final TopicPartition partition1 = new TopicPartition("XYZ-1", 1);
        final TopicPartition partition2 = new TopicPartition("XYZ-2", 2);
        Collection<TopicPartition> partitions = Arrays.asList(partition0, partition1, partition2);
        priorityKafkaConsumer.assign(partitions);
    }

    @Test
    public void testUnsubscribe() {
        doNothing().when(consumer0).unsubscribe();
        doNothing().when(consumer1).unsubscribe();
        doNothing().when(consumer2).unsubscribe();
        priorityKafkaConsumer.unsubscribe();
        verify(consumer0).unsubscribe();
        verify(consumer1).unsubscribe();
        verify(consumer2).unsubscribe();
    }

    @Test
    public void testCommitSync() {
        doNothing().when(consumer0).commitSync();
        doNothing().when(consumer1).commitSync();
        doNothing().when(consumer2).commitSync();
        priorityKafkaConsumer.commitSync();
        verify(consumer0).commitSync();
        verify(consumer1).commitSync();
        verify(consumer2).commitSync();
    }

    @Test
    public void testCommitSyncOffsets() {
        final TopicPartition partition0 = new TopicPartition("XYZ-0", 0);
        final TopicPartition partition1 = new TopicPartition("XYZ-1", 1);
        final TopicPartition partition2 = new TopicPartition("XYZ-2", 2);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Map<TopicPartition, OffsetAndMetadata> offsets
                        = (Map<TopicPartition, OffsetAndMetadata>) invocationOnMock.getArguments()[0];
                assertTrue(offsets.containsKey(partition0));
                assertEquals(1, offsets.size());
                return null;
            }
        }).when(consumer0).commitSync(any(Map.class));
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Map<TopicPartition, OffsetAndMetadata> offsets
                        = (Map<TopicPartition, OffsetAndMetadata>) invocationOnMock.getArguments()[0];
                assertTrue(offsets.containsKey(partition1));
                assertEquals(1, offsets.size());
                return null;
            }
        }).when(consumer1).commitSync(any(Map.class));
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Map<TopicPartition, OffsetAndMetadata> offsets
                        = (Map<TopicPartition, OffsetAndMetadata>) invocationOnMock.getArguments()[0];
                assertTrue(offsets.containsKey(partition2));
                assertEquals(1, offsets.size());
                return null;
            }
        }).when(consumer2).commitSync(any(Map.class));
        Collection<TopicPartition> partitions = Arrays.asList(partition0, partition1, partition2);
        Map<TopicPartition, OffsetAndMetadata> offsets
                = new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(partition0, null);
            put(partition1, null);
            put(partition2, null);
        }};
        priorityKafkaConsumer.commitSync(offsets);
        verify(consumer0).commitSync(any(Map.class));
        verify(consumer1).commitSync(any(Map.class));
        verify(consumer2).commitSync(any(Map.class));
    }

    @Test
    public void testCommitAsync() {
        doNothing().when(consumer0).commitAsync();
        doNothing().when(consumer1).commitAsync();
        doNothing().when(consumer2).commitAsync();
        priorityKafkaConsumer.commitAsync();
        verify(consumer0).commitAsync();
        verify(consumer1).commitAsync();
        verify(consumer2).commitAsync();
    }

    @Test
    public void testSeek() {
        TopicPartition partition1 = new TopicPartition("XYZ-1", 1);
        doNothing().when(consumer1).seek(partition1, 9);
        priorityKafkaConsumer.seek(partition1, 9);
        verify(consumer1).seek(partition1, 9);
    }

    @Test
    public void testSeekToBeginning() {
        TopicPartition partition0 = new TopicPartition("XYZ-0", 0);
        TopicPartition partition1 = new TopicPartition("XYZ-1", 1);
        TopicPartition partition2 = new TopicPartition("XYZ-2", 2);
        doNothing().when(consumer0).seekToBeginning(Arrays.asList(partition0));
        doNothing().when(consumer1).seekToBeginning(Arrays.asList(partition1));
        doNothing().when(consumer2).seekToBeginning(Arrays.asList(partition2));
        priorityKafkaConsumer.seekToBeginning(Arrays.asList(partition0, partition1, partition2));
        verify(consumer0).seekToBeginning(Arrays.asList(partition0));
        verify(consumer1).seekToBeginning(Arrays.asList(partition1));
        verify(consumer2).seekToBeginning(Arrays.asList(partition2));
    }

    @Test
    public void testSeekToEnd() {
        TopicPartition partition0 = new TopicPartition("XYZ-0", 0);
        TopicPartition partition1 = new TopicPartition("XYZ-1", 1);
        TopicPartition partition2 = new TopicPartition("XYZ-2", 2);
        doNothing().when(consumer0).seekToEnd(Arrays.asList(partition0));
        doNothing().when(consumer1).seekToEnd(Arrays.asList(partition1));
        doNothing().when(consumer2).seekToEnd(Arrays.asList(partition2));
        priorityKafkaConsumer.seekToEnd(Arrays.asList(partition0, partition1, partition2));
        verify(consumer0).seekToEnd(Arrays.asList(partition0));
        verify(consumer1).seekToEnd(Arrays.asList(partition1));
        verify(consumer2).seekToEnd(Arrays.asList(partition2));
    }

    @Test
    public void testPosition() {
        TopicPartition partition2 = new TopicPartition("XYZ-2", 2);
        when(consumer2.position(partition2)).thenReturn(9L);
        assertEquals(9, priorityKafkaConsumer.position(partition2));
        verify(consumer2).position(partition2);
    }

    @Test
    public void testCommitted() {
        TopicPartition partition0 = new TopicPartition("XYZ-0", 0);
        OffsetAndMetadata offset = new OffsetAndMetadata(9, "meta");
        when(consumer0.committed(partition0)).thenReturn(offset);
        assertEquals(offset, priorityKafkaConsumer.committed(partition0));
        verify(consumer0).committed(partition0);
    }

    @Test
    public void testMetrics() {
        final MetricName metric0 = new MetricName("n0", "g0", "d0", new HashMap<String, String>());
        final MetricName metric1 = new MetricName("n1", "g1", "d1", new HashMap<String, String>());
        final MetricName metric2 = new MetricName("n2", "g2", "d2", new HashMap<String, String>());
        final Map<MetricName, KafkaMetric> metrics0 = new HashMap<MetricName, KafkaMetric>() {{
            put(metric0, null);
        }};
        final Map<MetricName, KafkaMetric> metrics1 = new HashMap<MetricName, KafkaMetric>() {{
            put(metric1, null);
        }};
        final Map<MetricName, KafkaMetric> metrics2 = new HashMap<MetricName, KafkaMetric>() {{
            put(metric2, null);
        }};
        doReturn(metrics0).when(consumer0).metrics();
        doReturn(metrics1).when(consumer1).metrics();
        doReturn(metrics2).when(consumer2).metrics();
        Map<MetricName, KafkaMetric> expected = new HashMap<MetricName, KafkaMetric>() {{
            putAll(metrics0);
            putAll(metrics1);
            putAll(metrics2);
        }};
        assertEquals(expected, priorityKafkaConsumer.metrics());
        verify(consumer0).metrics();
        verify(consumer1).metrics();
        verify(consumer2).metrics();
    }

    @Test
    public void testPartitionsFor() {
        PartitionInfo partitionInfo0 = new PartitionInfo("XYZ-0", 0, null, null, null);
        PartitionInfo partitionInfo1 = new PartitionInfo("XYZ-1", 1, null, null, null);
        PartitionInfo partitionInfo2 = new PartitionInfo("XYZ-2", 2, null, null, null);
        when(consumer0.partitionsFor("XYZ-0")).thenReturn(Arrays.asList(partitionInfo0));
        when(consumer1.partitionsFor("XYZ-1")).thenReturn(Arrays.asList(partitionInfo1));
        when(consumer2.partitionsFor("XYZ-2")).thenReturn(Arrays.asList(partitionInfo2));
        List<PartitionInfo> expected = Arrays.asList(partitionInfo0, partitionInfo1, partitionInfo2);
        assertEquals(expected, priorityKafkaConsumer.partitionsFor("XYZ"));
        verify(consumer0).partitionsFor("XYZ-0");
        verify(consumer1).partitionsFor("XYZ-1");
        verify(consumer2).partitionsFor("XYZ-2");
    }

    @Test
    public void testListTopics() {
        when(consumer0.listTopics()).thenReturn(new HashMap<String, List<PartitionInfo>>() {{
            put("XYZ-0", null);
        }});
        when(consumer1.listTopics()).thenReturn(new HashMap<String, List<PartitionInfo>>() {{
            put("XYZ-1", null);
        }});
        when(consumer2.listTopics()).thenReturn(new HashMap<String, List<PartitionInfo>>() {{
            put("XYZ-2", null);
        }});
        assertEquals(new HashMap<String, List<PartitionInfo>>() {{
            put("XYZ-0", null);
            put("XYZ-1", null);
            put("XYZ-2", null);
        }}, priorityKafkaConsumer.listTopics());
        verify(consumer0).listTopics();
        verify(consumer1).listTopics();
        verify(consumer2).listTopics();
    }

    @Test
    public void testPaused() {
        final TopicPartition partition0 = new TopicPartition("XYZ-0", 0);
        final TopicPartition partition1 = new TopicPartition("XYZ-1", 1);
        final TopicPartition partition2 = new TopicPartition("XYZ-2", 2);
        when(consumer0.paused()).thenReturn(new HashSet<TopicPartition>() {{
            add(partition0);
        }});
        when(consumer1.paused()).thenReturn(new HashSet<TopicPartition>() {{
            add(partition1);
        }});
        when(consumer2.paused()).thenReturn(new HashSet<TopicPartition>() {{
            add(partition2);
        }});
        Set<TopicPartition> expected = new HashSet<TopicPartition>() {{
            add(partition0);
            add(partition1);
            add(partition2);
        }};
        assertEquals(expected, priorityKafkaConsumer.paused());
        verify(consumer0).paused();
        verify(consumer1).paused();
        verify(consumer2).paused();
    }

    @Test
    public void testPause() {
        TopicPartition partition0 = new TopicPartition("XYZ-0", 0);
        TopicPartition partition1 = new TopicPartition("XYZ-1", 1);
        TopicPartition partition2 = new TopicPartition("XYZ-2", 2);
        doNothing().when(consumer0).pause(Arrays.asList(partition0));
        doNothing().when(consumer1).pause(Arrays.asList(partition1));
        doNothing().when(consumer2).pause(Arrays.asList(partition2));
        priorityKafkaConsumer.pause(Arrays.asList(partition0, partition1, partition2));
        verify(consumer0).pause(Arrays.asList(partition0));
        verify(consumer1).pause(Arrays.asList(partition1));
        verify(consumer2).pause(Arrays.asList(partition2));
    }

    @Test
    public void testResume() {
        TopicPartition partition0 = new TopicPartition("XYZ-0", 0);
        TopicPartition partition1 = new TopicPartition("XYZ-1", 1);
        TopicPartition partition2 = new TopicPartition("XYZ-2", 2);
        doNothing().when(consumer0).resume(Arrays.asList(partition0));
        doNothing().when(consumer1).resume(Arrays.asList(partition1));
        doNothing().when(consumer2).resume(Arrays.asList(partition2));
        priorityKafkaConsumer.resume(Arrays.asList(partition0, partition1, partition2));
        verify(consumer0).resume(Arrays.asList(partition0));
        verify(consumer1).resume(Arrays.asList(partition1));
        verify(consumer2).resume(Arrays.asList(partition2));
    }

    @Test
    public void testOffsetsForTimes() {
        final TopicPartition partition0 = new TopicPartition("XYZ-0", 0);
        final TopicPartition partition1 = new TopicPartition("XYZ-1", 1);
        final TopicPartition partition2 = new TopicPartition("XYZ-2", 2);
        Map<TopicPartition, Long> timestampsToSearch0 = new HashMap<TopicPartition, Long>() {{
            put(partition0, null);
        }};
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes0
                = new HashMap<TopicPartition, OffsetAndTimestamp>() {{
            put(partition0, null);
        }};
        when(consumer0.offsetsForTimes(timestampsToSearch0)).thenReturn(offsetsForTimes0);
        Map<TopicPartition, Long> timestampsToSearch1 = new HashMap<TopicPartition, Long>() {{
            put(partition1, null);
        }};
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes1
                = new HashMap<TopicPartition, OffsetAndTimestamp>() {{
            put(partition1, null);
        }};
        when(consumer1.offsetsForTimes(timestampsToSearch1)).thenReturn(offsetsForTimes1);
        Map<TopicPartition, Long> timestampsToSearch2 = new HashMap<TopicPartition, Long>() {{
            put(partition2, null);
        }};
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes2
                = new HashMap<TopicPartition, OffsetAndTimestamp>() {{
            put(partition2, null);
        }};
        when(consumer2.offsetsForTimes(timestampsToSearch2)).thenReturn(offsetsForTimes2);

        Map<TopicPartition, Long> timestampsToSearch = new HashMap<TopicPartition, Long>() {{
            put(partition0, null);
            put(partition1, null);
            put(partition2, null);
        }};
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes
                = new HashMap<TopicPartition, OffsetAndTimestamp>() {{
            put(partition0, null);
            put(partition1, null);
            put(partition2, null);
        }};
        assertEquals(offsetsForTimes, priorityKafkaConsumer.offsetsForTimes(timestampsToSearch));
        verify(consumer0).offsetsForTimes(timestampsToSearch0);
        verify(consumer1).offsetsForTimes(timestampsToSearch1);
        verify(consumer2).offsetsForTimes(timestampsToSearch2);
    }

    @Test
    public void testBeginningOffsets() {
        final TopicPartition partition0 = new TopicPartition("XYZ-0", 0);
        final TopicPartition partition1 = new TopicPartition("XYZ-1", 1);
        final TopicPartition partition2 = new TopicPartition("XYZ-2", 2);
        List<TopicPartition> partitions0 = Arrays.asList(partition0);
        when(consumer0.beginningOffsets(partitions0)).thenReturn(new HashMap<TopicPartition, Long>() {{
            put(partition0, null);
        }});
        List<TopicPartition> partitions1 = Arrays.asList(partition1);
        when(consumer1.beginningOffsets(partitions1)).thenReturn(new HashMap<TopicPartition, Long>() {{
            put(partition1, null);
        }});
        List<TopicPartition> partitions2 = Arrays.asList(partition2);
        when(consumer2.beginningOffsets(partitions2)).thenReturn(new HashMap<TopicPartition, Long>() {{
            put(partition2, null);
        }});
        Map<TopicPartition, Long> expected = new HashMap<TopicPartition, Long>() {{
            put(partition0, null);
            put(partition1, null);
            put(partition2, null);
        }};
        assertEquals(expected, priorityKafkaConsumer.beginningOffsets(
                Arrays.asList(partition0, partition1, partition2)));
        verify(consumer0).beginningOffsets(partitions0);
        verify(consumer1).beginningOffsets(partitions1);
        verify(consumer2).beginningOffsets(partitions2);
    }

    @Test
    public void testEndOffsets() {
        final TopicPartition partition0 = new TopicPartition("XYZ-0", 0);
        final TopicPartition partition1 = new TopicPartition("XYZ-1", 1);
        final TopicPartition partition2 = new TopicPartition("XYZ-2", 2);
        List<TopicPartition> partitions0 = Arrays.asList(partition0);
        when(consumer0.endOffsets(partitions0)).thenReturn(new HashMap<TopicPartition, Long>() {{
            put(partition0, null);
        }});
        List<TopicPartition> partitions1 = Arrays.asList(partition1);
        when(consumer1.endOffsets(partitions1)).thenReturn(new HashMap<TopicPartition, Long>() {{
            put(partition1, null);
        }});
        List<TopicPartition> partitions2 = Arrays.asList(partition2);
        when(consumer2.endOffsets(partitions2)).thenReturn(new HashMap<TopicPartition, Long>() {{
            put(partition2, null);
        }});
        Map<TopicPartition, Long> expected = new HashMap<TopicPartition, Long>() {{
            put(partition0, null);
            put(partition1, null);
            put(partition2, null);
        }};
        assertEquals(expected, priorityKafkaConsumer.endOffsets(
                Arrays.asList(partition0, partition1, partition2)));
        verify(consumer0).endOffsets(partitions0);
        verify(consumer1).endOffsets(partitions1);
        verify(consumer2).endOffsets(partitions2);
    }

    @Test
    public void testClose() {
        doNothing().when(consumer0).close();
        doNothing().when(consumer1).close();
        doNothing().when(consumer2).close();
        priorityKafkaConsumer.close();
        verify(consumer0).close();
        verify(consumer1).close();
        verify(consumer2).close();
    }

    @Test
    public void testWakeup() {
        doNothing().when(consumer0).wakeup();
        doNothing().when(consumer1).wakeup();
        doNothing().when(consumer2).wakeup();
        priorityKafkaConsumer.wakeup();
        verify(consumer0).wakeup();
        verify(consumer1).wakeup();
        verify(consumer2).wakeup();
    }

    @Test
    public void testPollNoBurstEmptyWindows() {
        Map<Integer, Window> consumerPollWindowHistory = new HashMap<Integer, Window>();
        Map<Integer, Integer> distribution = ExpMaxPollRecordsDistributor.instance()
                .distribution(maxPriority, maxPollRecords);
        for (int i = 0; i < maxPriority; ++i) {
            consumerPollWindowHistory.put(i, new Window(ClientConfigs.DEFAULT_MAX_POLL_HISTORY_WINDOW_SIZE,
                    ClientConfigs.DEFAULT_MIN_POLL_WINDOW_MAXOUT_SIZE, distribution.get(i)));
        }
        priorityKafkaConsumer = new CapacityBurstPriorityKafkaConsumer<Integer, String>(
                maxPriority, maxPollRecords, consumers, consumerPollWindowHistory);
        final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records0
                = new HashMap<TopicPartition, List<ConsumerRecord<Integer, String>>>() {{
            put(new TopicPartition("XYZ-0", 0),
                    Arrays.asList(new ConsumerRecord<Integer, String>("XYZ-0", 0, 0, 0, "0")));
        }};
        ConsumerRecords<Integer, String> consumerRecords0 = new ConsumerRecords<Integer, String>(records0);
        when(consumer0.poll(0)).thenReturn(consumerRecords0);
        final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records1
                = new HashMap<TopicPartition, List<ConsumerRecord<Integer, String>>>() {{
            put(new TopicPartition("XYZ-1", 1),
                    Arrays.asList(new ConsumerRecord<Integer, String>("XYZ-1", 1, 1, 1, "1")));
        }};
        ConsumerRecords<Integer, String> consumerRecords1 = new ConsumerRecords<Integer, String>(records1);
        when(consumer1.poll(0)).thenReturn(consumerRecords1);
        final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records2
                = new HashMap<TopicPartition, List<ConsumerRecord<Integer, String>>>() {{
            put(new TopicPartition("XYZ-2", 2),
                    Arrays.asList(new ConsumerRecord<Integer, String>("XYZ-2", 2, 2, 2, "2")));
        }};
        ConsumerRecords<Integer, String> consumerRecords2 = new ConsumerRecords<Integer, String>(records2);
        when(consumer2.poll(0)).thenReturn(consumerRecords2);
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> expected
                = new HashMap<TopicPartition, List<ConsumerRecord<Integer, String>>>() {{
            putAll(records0);
            putAll(records1);
            putAll(records2);
        }};
        ConsumerRecords<Integer, String> consumerRecords = priorityKafkaConsumer.poll(0);
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> actual
                = new HashMap<TopicPartition, List<ConsumerRecord<Integer, String>>>();
        for (TopicPartition partition : consumerRecords.partitions()) {
            actual.put(partition, consumerRecords.records(partition));
        }
        assertEquals(expected, actual);
        verify(consumer0).poll(0);
        verify(consumer1).poll(0);
        verify(consumer2).poll(0);
    }

    @Test
    public void testPollNoBurstLowestPriority() {
        Map<Integer, Window> consumerPollWindowHistory = new HashMap<Integer, Window>();
        Map<Integer, Integer> distribution = ExpMaxPollRecordsDistributor.instance()
                .distribution(maxPriority, maxPollRecords);
        for (int i = 0; i < maxPriority; ++i) {
            consumerPollWindowHistory.put(i, new Window(ClientConfigs.DEFAULT_MAX_POLL_HISTORY_WINDOW_SIZE,
                    ClientConfigs.DEFAULT_MIN_POLL_WINDOW_MAXOUT_SIZE, distribution.get(i)));
        }
        for (int i = 0; i < ClientConfigs.DEFAULT_MAX_POLL_HISTORY_WINDOW_SIZE; ++i) {
            consumerPollWindowHistory.get(0).add(7);
            consumerPollWindowHistory.get(1).add(10);
            consumerPollWindowHistory.get(2).add(25);
        }
        for (int i = 0; i < 3; ++i) {
            consumerPollWindowHistory.get(1).add(i);
            consumerPollWindowHistory.get(2).add(i);
        }
        priorityKafkaConsumer = new CapacityBurstPriorityKafkaConsumer<Integer, String>(
                maxPriority, maxPollRecords, consumers, consumerPollWindowHistory);
        CapacityBurstPriorityKafkaConsumer<Integer, String> spyPriorityKafkaConsumer
                = spy(priorityKafkaConsumer);
        final int[] counter = {0};
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                KafkaConsumer<Integer, String> consumer
                        = (KafkaConsumer<Integer, String>) invocationOnMock.getArguments()[0];
                int maxPollRecords = (Integer) invocationOnMock.getArguments()[1];
                assertTrue(consumer == consumer0);
                if (counter[0] == 0) {
                    assertEquals(15, maxPollRecords);
                } else {
                    assertEquals(7, maxPollRecords);
                }
                ++counter[0];
                return null;
            }
        }).when(spyPriorityKafkaConsumer).updateMaxPollRecords(any(KafkaConsumer.class), any(Integer.class));
        final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records0
                = new HashMap<TopicPartition, List<ConsumerRecord<Integer, String>>>() {{
            put(new TopicPartition("XYZ-0", 0),
                    Arrays.asList(new ConsumerRecord<Integer, String>("XYZ-0", 0, 0, 0, "0")));
        }};
        ConsumerRecords<Integer, String> consumerRecords0 = new ConsumerRecords<Integer, String>(records0);
        when(consumer0.poll(0)).thenReturn(consumerRecords0);
        final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records1
                = new HashMap<TopicPartition, List<ConsumerRecord<Integer, String>>>() {{
            put(new TopicPartition("XYZ-1", 1),
                    Arrays.asList(new ConsumerRecord<Integer, String>("XYZ-1", 1, 1, 1, "1")));
        }};
        ConsumerRecords<Integer, String> consumerRecords1 = new ConsumerRecords<Integer, String>(records1);
        when(consumer1.poll(0)).thenReturn(consumerRecords1);
        final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records2
                = new HashMap<TopicPartition, List<ConsumerRecord<Integer, String>>>() {{
            put(new TopicPartition("XYZ-2", 2),
                    Arrays.asList(new ConsumerRecord<Integer, String>("XYZ-2", 2, 2, 2, "2")));
        }};
        ConsumerRecords<Integer, String> consumerRecords2 = new ConsumerRecords<Integer, String>(records2);
        when(consumer2.poll(0)).thenReturn(consumerRecords2);
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> expected
                = new HashMap<TopicPartition, List<ConsumerRecord<Integer, String>>>() {{
            putAll(records0);
            putAll(records1);
            putAll(records2);
        }};
        doCallRealMethod().when(spyPriorityKafkaConsumer).poll(0);
        ConsumerRecords<Integer, String> consumerRecords = spyPriorityKafkaConsumer.poll(0);
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> actual
                = new HashMap<TopicPartition, List<ConsumerRecord<Integer, String>>>();
        for (TopicPartition partition : consumerRecords.partitions()) {
            actual.put(partition, consumerRecords.records(partition));
        }
        assertEquals(expected, actual);
        verify(consumer0).poll(0);
        verify(consumer1).poll(0);
        verify(consumer2).poll(0);
        verify(spyPriorityKafkaConsumer).poll(0);
        verify(spyPriorityKafkaConsumer, times(2))
                .updateMaxPollRecords(any(KafkaConsumer.class), any(Integer.class));
    }

    @Test
    public void testPollNoBurstHigherPriority() {
        Map<Integer, Window> consumerPollWindowHistory = new HashMap<Integer, Window>();
        Map<Integer, Integer> distribution = ExpMaxPollRecordsDistributor.instance()
                .distribution(maxPriority, maxPollRecords);
        for (int i = 0; i < maxPriority; ++i) {
            consumerPollWindowHistory.put(i, new Window(ClientConfigs.DEFAULT_MAX_POLL_HISTORY_WINDOW_SIZE,
                    ClientConfigs.DEFAULT_MIN_POLL_WINDOW_MAXOUT_SIZE, distribution.get(i)));
        }
        for (int i = 0; i < ClientConfigs.DEFAULT_MAX_POLL_HISTORY_WINDOW_SIZE; ++i) {
            consumerPollWindowHistory.get(0).add(5);
            consumerPollWindowHistory.get(1).add(14);
            consumerPollWindowHistory.get(2).add(25);
        }
        priorityKafkaConsumer = new CapacityBurstPriorityKafkaConsumer<Integer, String>(
                maxPriority, maxPollRecords, consumers, consumerPollWindowHistory);
        CapacityBurstPriorityKafkaConsumer<Integer, String> spyPriorityKafkaConsumer
                = spy(priorityKafkaConsumer);
        final int[] counter = {0};
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                KafkaConsumer<Integer, String> consumer
                        = (KafkaConsumer<Integer, String>) invocationOnMock.getArguments()[0];
                int maxPollRecords = (Integer) invocationOnMock.getArguments()[1];
                assertTrue(consumer == consumer1);
                if (counter[0] == 0) {
                    assertEquals(20, maxPollRecords);
                } else {
                    assertEquals(14, maxPollRecords);
                }
                ++counter[0];
                return null;
            }
        }).when(spyPriorityKafkaConsumer).updateMaxPollRecords(any(KafkaConsumer.class), any(Integer.class));
        final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records0
                = new HashMap<TopicPartition, List<ConsumerRecord<Integer, String>>>() {{
            put(new TopicPartition("XYZ-0", 0),
                    Arrays.asList(new ConsumerRecord<Integer, String>("XYZ-0", 0, 0, 0, "0")));
        }};
        ConsumerRecords<Integer, String> consumerRecords0 = new ConsumerRecords<Integer, String>(records0);
        when(consumer0.poll(0)).thenReturn(consumerRecords0);
        final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records1
                = new HashMap<TopicPartition, List<ConsumerRecord<Integer, String>>>() {{
            put(new TopicPartition("XYZ-1", 1),
                    Arrays.asList(new ConsumerRecord<Integer, String>("XYZ-1", 1, 1, 1, "1")));
        }};
        ConsumerRecords<Integer, String> consumerRecords1 = new ConsumerRecords<Integer, String>(records1);
        when(consumer1.poll(0)).thenReturn(consumerRecords1);
        final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records2
                = new HashMap<TopicPartition, List<ConsumerRecord<Integer, String>>>() {{
            put(new TopicPartition("XYZ-2", 2),
                    Arrays.asList(new ConsumerRecord<Integer, String>("XYZ-2", 2, 2, 2, "2")));
        }};
        ConsumerRecords<Integer, String> consumerRecords2 = new ConsumerRecords<Integer, String>(records2);
        when(consumer2.poll(0)).thenReturn(consumerRecords2);
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> expected
                = new HashMap<TopicPartition, List<ConsumerRecord<Integer, String>>>() {{
            putAll(records0);
            putAll(records1);
            putAll(records2);
        }};
        doCallRealMethod().when(spyPriorityKafkaConsumer).poll(0);
        ConsumerRecords<Integer, String> consumerRecords = spyPriorityKafkaConsumer.poll(0);
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> actual
                = new HashMap<TopicPartition, List<ConsumerRecord<Integer, String>>>();
        for (TopicPartition partition : consumerRecords.partitions()) {
            actual.put(partition, consumerRecords.records(partition));
        }
        assertEquals(expected, actual);
        verify(consumer0).poll(0);
        verify(consumer1).poll(0);
        verify(consumer2).poll(0);
        verify(spyPriorityKafkaConsumer).poll(0);
        verify(spyPriorityKafkaConsumer, times(2))
                .updateMaxPollRecords(any(KafkaConsumer.class), any(Integer.class));
    }
}
