package com.sohu.adrd.kafka2hdfs.util;

import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sohu.adrd.kafka2hdfs.kafka.FailedFetchException;
import com.sohu.adrd.kafka2hdfs.kafka.KafkaConfig;
import com.sohu.adrd.kafka2hdfs.kafka.KafkaError;
import com.sohu.adrd.kafka2hdfs.kafka.Partition;

public class Utils {
	public static final Logger LOG = LoggerFactory.getLogger(Utils.class);
	private static final int NO_OFFSET = -5;

	public static long getOffset(SimpleConsumer consumer, String topic, int partition, KafkaConfig config) {
		long startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		if ( config.forceFromStart ) {
			startOffsetTime = config.startOffsetTime;
		}
		return getOffset(consumer, topic, partition, startOffsetTime);
	}

	public static long getOffset(SimpleConsumer consumer, String topic, int partition, long startOffsetTime) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(startOffsetTime, 1));
		OffsetRequest request = new OffsetRequest(
				requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());

		long[] offsets = consumer.getOffsetsBefore(request).offsets(topic, partition);
		if (offsets.length > 0) {
			return offsets[0];
		} else {
			return NO_OFFSET;
		}
		
	}
	
	public static ByteBufferMessageSet fetchMessages(KafkaConfig config, SimpleConsumer consumer, Partition partition, long offset) {
        ByteBufferMessageSet msgs = null;
        String topic = config.topic;
        int partitionId = partition.partition;
        for (int errors = 0; errors < 2 && msgs == null; errors++) {
            FetchRequestBuilder builder = new FetchRequestBuilder();
            FetchRequest fetchRequest = builder.addFetch(topic, partitionId, offset, config.fetchSizeBytes).
                    clientId(config.clientId).build();
            FetchResponse fetchResponse;
            try {
                fetchResponse = consumer.fetch(fetchRequest);
            } catch (Exception e) {
                if (e instanceof ConnectException) {
                    throw new FailedFetchException(e);
                } else {
                    throw new RuntimeException(e);
                }
            }
            if (fetchResponse.hasError()) {
                KafkaError error = KafkaError.getError(fetchResponse.errorCode(topic, partitionId));
                if (error.equals(KafkaError.OFFSET_OUT_OF_RANGE) && config.useStartOffsetTimeIfOffsetOutOfRange && errors == 0) {
                    long startOffset = getOffset(consumer, topic, partitionId, config.startOffsetTime);
                    LOG.warn("Got fetch request with offset out of range: [" + offset + "]; " +
                            "retrying with default start offset time from configuration. " +
                            "configured start offset time: [" + config.startOffsetTime + "] offset: [" + startOffset + "]");
                    offset = startOffset;
                } else {
                    String message = "Error fetching data from [" + partition + "] for topic [" + topic + "]: [" + error + "]";
                    LOG.error(message);
                    throw new FailedFetchException(message);
                }
            } else {
                msgs = fetchResponse.messageSet(topic, partitionId);
            }
        }
        return msgs;
    }
	
    public static ByteBuffer getLog(Message msg) {
        return msg.payload();
    }

}
