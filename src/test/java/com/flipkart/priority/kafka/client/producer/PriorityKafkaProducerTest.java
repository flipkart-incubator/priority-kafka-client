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
 */package com.flipkart.priority.kafka.client.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class PriorityKafkaProducerTest {

    private int maxPriority = 3;
    private KafkaProducer<Integer, String> producer;
    private PriorityKafkaProducer<Integer, String> priorityKafkaProducer;

    @Before
    public void before() {
        producer = mock(KafkaProducer.class);
        priorityKafkaProducer = new PriorityKafkaProducer<Integer, String>(maxPriority, producer);
    }

    @After
    public void after() {
        verifyNoMoreInteractions(producer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingMaxPriorityConfigUsingProperties() {
        Properties properties = new Properties();
        new PriorityKafkaProducer<Integer, String>(properties);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingMaxPriorityConfigUsingMap() {
        Map<String, Object> configs = new HashMap<String, Object>();
        new PriorityKafkaProducer<Integer, String>(configs);
    }

    @Test
    public void testGetMaxPriority() {
        assertEquals(maxPriority, priorityKafkaProducer.getMaxPriority());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSendWithInvalidPriorityUsingHigherThanMaxPriority() {
        ProducerRecord<Integer, String> record
                = new ProducerRecord<Integer, String>("XYZ", null, 1, "value");
        Callback callback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
            }
        };
        priorityKafkaProducer.send(10, record, callback);
    }

    @Test
    public void testSend() {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ProducerRecord record = (ProducerRecord) invocationOnMock.getArguments()[0];
                assertEquals("XYZ-2", record.topic());
                assertEquals(9, record.partition().intValue());
                assertEquals(1, record.key());
                assertEquals("value", record.value());
                return null;
            }
        }).when(producer).send(any(ProducerRecord.class));

        ProducerRecord<Integer, String> record
                = new ProducerRecord<Integer, String>("XYZ", 9, 1, "value");
        priorityKafkaProducer.send(2, record);

        verify(producer).send(any(ProducerRecord.class));
    }

    @Test
    public void testSendWithCallback() {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ProducerRecord record = (ProducerRecord) invocationOnMock.getArguments()[0];
                assertEquals("XYZ-0", record.topic());
                assertEquals(9, record.partition().intValue());
                assertEquals(1, record.key());
                assertEquals("value", record.value());
                assertTrue(invocationOnMock.getArguments()[1] instanceof Callback);
                return null;
            }
        }).when(producer).send(any(ProducerRecord.class), any(Callback.class));

        ProducerRecord<Integer, String> record
                = new ProducerRecord<Integer, String>("XYZ", 9, 1, "value");
        Callback callback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
            }
        };
        priorityKafkaProducer.send(record, callback);

        verify(producer).send(any(ProducerRecord.class), any(Callback.class));
    }

    @Test
    public void testFlush() {
        doNothing().when(producer).flush();
        priorityKafkaProducer.flush();
        verify(producer).flush();
    }

    @Test
    public void testPartitionsFor() {
        PartitionInfo partition0 = new PartitionInfo("XYZ-0", 0, null, null, null);
        PartitionInfo partition1 = new PartitionInfo("XYZ-1", 1, null, null, null);
        PartitionInfo partition2 = new PartitionInfo("XYZ-2", 2, null, null, null);
        when(producer.partitionsFor("XYZ-0")).thenReturn(Arrays.asList(partition0));
        when(producer.partitionsFor("XYZ-1")).thenReturn(Arrays.asList(partition1));
        when(producer.partitionsFor("XYZ-2")).thenReturn(Arrays.asList(partition2));
        List<PartitionInfo> actual = priorityKafkaProducer.partitionsFor("XYZ");
        List<PartitionInfo> expected = Arrays.asList(partition0, partition1, partition2);
        assertArrayEquals(expected.toArray(), actual.toArray());
        verify(producer).partitionsFor("XYZ-0");
        verify(producer).partitionsFor("XYZ-1");
        verify(producer).partitionsFor("XYZ-2");
    }

    @Test
    public void testMetrics() {
        Map<MetricName, KafkaMetric> metrics = new HashMap<MetricName, KafkaMetric>();
        doReturn(metrics).when(producer).metrics();
        assertTrue(metrics == priorityKafkaProducer.metrics());
        verify(producer).metrics();
    }

    @Test
    public void testClose() {
        doNothing().when(producer).close();
        priorityKafkaProducer.close();
        verify(producer).close();
    }

    @Test
    public void testCloseWithTimeout() {
        doNothing().when(producer).close(2, TimeUnit.MINUTES);
        priorityKafkaProducer.close(2, TimeUnit.MINUTES);
        verify(producer).close(2, TimeUnit.MINUTES);
    }
}
