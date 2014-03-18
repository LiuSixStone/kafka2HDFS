package com.sohu.adrd.kafka2hdfs.kafka;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.sohu.adrd.kafka2hdfs.util.ConstData;
import com.sohu.adrd.kafka2hdfs.util.GetLogTime;
import com.sohu.adrd.kafka2hdfs.util.HDFSWriter;
import com.sohu.adrd.kafka2hdfs.util.Utils;

public class PartitionManager {
	public static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);
	
    static class KafkaMessageId {
        public Partition partition;
        public long offset;

        public KafkaMessageId(Partition partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }
    
    public static class MessageAndRealOffset {
        public Message msg;
        public long offset;

        public MessageAndRealOffset(Message msg, long offset) {
            this.msg = msg;
            this.offset = offset;
        }
    }

    static enum EmitState {
        EMITTED_SUCESSFUL,
        EMITTED_FAILED,
        NO_EMITTED
    }
    
    private Long emittedToOffset;
    private Long committedTo;
    private Long oldOffset;
    private Partition partition;
    private Kafka2hdfsConfig kafka2hdfsConfig;
    private SimpleConsumer consumer;
    private DynamicPartitionConnections connections;
    private ZkState state;
    
    private long startOffset = -1;
    private long endOffset = -1;
    
    private volatile boolean needCheckTime = true;
    private boolean hasDone = false;
    private StringBuilder sb;//for buffer messages
    private Timer checkDateTimer;
    private GetLogTime timerCheck;
    private String dateString;
    private HDFSWriter writer;
    
    //private Writer fr;
    
    public PartitionManager(DynamicPartitionConnections connections, ZkState state, Kafka2hdfsConfig kafka2hdfsConfig, Partition partition) {
        this.partition = partition;
        this.connections = connections;
        this.kafka2hdfsConfig = kafka2hdfsConfig;
        this.state = state;
        //get offset to pull
        startOffset = kafka2hdfsConfig.offsetList.get(partition.partition).startOffset;
        endOffset = kafka2hdfsConfig.offsetList.get(partition.partition).endOffset;
        LOG.info("partion: " + partition.partition + " startOffset: " + startOffset + " endOffset: " + endOffset);
        sb = new StringBuilder(kafka2hdfsConfig.bufferSizeBytes);
      /*  try {
			fr = new FileWriter("e:/src/Kafka2HDFS/messages.txt");
		} catch (IOException e) {
			// TODO Auto-generated catch block
		}*/
        
        init();
    }
    
    private void init() {
        consumer = connections.register(partition.host, partition.partition);
        
        checkDateTimer = new Timer();
        checkDateTimer.scheduleAtFixedRate(new CheckTime(), new Date(), ConstData.DATE_CHECK_SEC*1000L);
        
        timerCheck = new GetLogTime();
        writer = new HDFSWriter(kafka2hdfsConfig.outDir, kafka2hdfsConfig.fileSize, kafka2hdfsConfig.compressor, partition.partition);
        
        Long jsonOffset = null;
        String path = committedPath();
        try {
            Map<Object, Object> json = state.readJSON(path);
            LOG.info("Read partition information from: " + path +  "  --> " + json );
            if (json != null) {
                jsonOffset = (Long) json.get("offset");
            }
        } catch (Throwable e) {
            LOG.warn("Error reading and/or parsing at ZkNode: " + path, e);
        }

        if (jsonOffset == null) { // failed to parse JSON?
        	if (-1 != startOffset) {
				committedTo = kafka2hdfsConfig.offsetList.get(partition.partition).startOffset;
				LOG.info("No partition information found, using user configuration to determine offset");
			}else {
				committedTo = Utils.getOffset(consumer, kafka2hdfsConfig.topic, partition.partition, kafka2hdfsConfig);
	            LOG.info("No partition information found, using configuration to determine offset");
			}
        } else if (kafka2hdfsConfig.forceFromStart) {
            committedTo = Utils.getOffset(consumer, kafka2hdfsConfig.topic, partition.partition, kafka2hdfsConfig.startOffsetTime);
            LOG.info("Topology change detected and reset from start forced, using configuration to determine offset");
        } else {
        	if (-1 != startOffset) {
        		committedTo = startOffset;
                LOG.info("Read last commit offset from zookeeper: " + committedTo);
			}else {
				committedTo = jsonOffset;
				LOG.info("refresh last commit offset using user configuration: " + committedTo);
			}
        }

        LOG.info("Starting Kafka " + consumer.host() + ":" + partition.partition + " from offset " + committedTo);
        emittedToOffset = committedTo;
        oldOffset = committedTo;
    }
    
    public void commit() {
        LOG.info("Committing offset for " + partition);
        if (oldOffset != committedTo) {
            Map<Object, Object> data = (Map<Object, Object>) ImmutableMap.builder()
                    .put("offset", oldOffset)
                    .put("partition", partition.partition)
                    .put("broker", ImmutableMap.of("host", partition.host.host,
                            "port", partition.host.port))
                    .put("topic", kafka2hdfsConfig.topic).build();
            state.writeJSON(committedPath(), data);

            LOG.info("Wrote committed offset to ZK: " + oldOffset);
            committedTo = oldOffset;
        }
        LOG.info("Committed offset " + oldOffset + " for " + partition);
    }
    
    private String committedPath() {
        return kafka2hdfsConfig.zkRoot + "/" + kafka2hdfsConfig.id + "/" + partition.getId();
    }
    
    //returns false if it's reached the end of current batch
    public EmitState next() {
    	//has finished 
    	if (isDone()) {
    		return EmitState.NO_EMITTED;
    	}
    	//get and buffer messages
		fill();
    	//write to HDFS
		if (!writer.Writer(sb.toString(), dateString)) {
			LOG.error("failed write to hdfs!!");
			return EmitState.EMITTED_FAILED;
		}
		
		if (hasDone) {
			writer.close();
		}
    	/*try {
    		fr.write(sb.toString());
    		if (isDone()) {
				fr.close();
			}
    	} catch (Exception e) {
    		e.printStackTrace();
    		return EmitState.EMITTED_FAILED;
    	}*/
		LOG.info("write LOG into HDFS SUCESS!!");
    	ack(emittedToOffset);
    	return EmitState.EMITTED_SUCESSFUL;
    }
    
    private void fill() {
        ByteBufferMessageSet msgs = Utils.fetchMessages(kafka2hdfsConfig, consumer, partition, oldOffset);
        int numMessages = countMessages(msgs);

        if (numMessages > 0) {
            LOG.info("Fetched " + numMessages + " messages from Kafka: " + consumer.host() + ":" + partition.partition);
        }
        
        sb.delete(0, sb.length());
        for (MessageAndOffset msg : msgs) {
        	if (endOffset != -1 && emittedToOffset > endOffset) {
        		LOG.info("has reach the end of request!!");
        		hasDone = true;
        		break;
        	}
        	
        	ByteBuffer payload = Utils.getLog(msg.message());
    		byte[] bytes = new byte[payload.limit()];
    		payload.get(bytes);
    		sb.append(new String(bytes)).append('\n');
    		
    		if (needCheckTime) {
				dateString = timerCheck.parseDate(new String(bytes));
				needCheckTime = false;
			}
    		
            emittedToOffset = msg.nextOffset();
        }
        
        Long addMessage = emittedToOffset - oldOffset;
        if (addMessage > 0) {
            LOG.info("Added " + addMessage + " messages from Kafka: " + consumer.host() + ":" + partition.partition + " to internal buffers");
        }
    }
    
    private int countMessages(ByteBufferMessageSet messageSet) {
        int counter = 0;
        for (MessageAndOffset messageAndOffset : messageSet) {
            counter++;
            
        }
        return counter;
    }
    
    public void ack(Long offset) {
        oldOffset = offset;
    }
    
    public void close() {
        connections.unregister(partition.host, partition.partition);
    }
    
    public boolean isDone() { 
    	return hasDone;
    }
    
    private class CheckTime extends TimerTask {

		@Override
		public void run() {
			needCheckTime = true;
		}
    	
    }
}
