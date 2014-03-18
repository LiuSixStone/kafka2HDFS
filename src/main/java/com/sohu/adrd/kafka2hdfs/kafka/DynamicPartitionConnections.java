package com.sohu.adrd.kafka2hdfs.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.javaapi.consumer.SimpleConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicPartitionConnections {
	public static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionConnections.class);
	
    static class ConnectionInfo {
        SimpleConsumer consumer;
        Set<Integer> partitions = new HashSet();

        public ConnectionInfo(SimpleConsumer consumer) {
            this.consumer = consumer;
        }
    }
    
    Map<Broker, ConnectionInfo> connections = new HashMap();
    KafkaConfig config;
    ZkBrokerReader reader;
    
    public DynamicPartitionConnections(KafkaConfig config, ZkBrokerReader reader) {
        this.config = config;
        this.reader = reader;
    }
    
    public SimpleConsumer register(Partition partition) {
        Broker broker = reader.getCurrentBrokers().getBrokerFor(partition.partition);
        return register(broker, partition.partition);
    }
    
    public SimpleConsumer register(Broker host, int partition) {
    	
        if (!connections.containsKey(host)) {
            connections.put(host, new ConnectionInfo(new SimpleConsumer(host.host, host.port, config.socketTimeoutMs, config.bufferSizeBytes, config.clientId)));
        }
        ConnectionInfo info = connections.get(host);
        info.partitions.add(partition);
        return info.consumer;
    }
    
    public SimpleConsumer getConnection(Partition partition) {
        ConnectionInfo info = connections.get(partition.host);
        if (info != null) {
            return info.consumer;
        }
        return null;
    }

    public void unregister(Broker broker, int partition) {
        ConnectionInfo info = connections.get(broker);
        info.partitions.remove(partition);
        if (info.partitions.isEmpty()) {
            info.consumer.close();
            connections.remove(broker);
        }
    }
    
    public void unregister(Partition partition) {
        unregister(partition.host, partition.partition);
    }
    
    public void clear() {
        for (ConnectionInfo info : connections.values()) {
            info.consumer.close();
        }
        connections.clear();
    }
    
    public static void main(String[] args) {
    	SimpleConsumer consumer = new SimpleConsumer("10.16.10.200", 9092, 10000, 1024*1024, "kajlaafvm");
    	LOG.info("create sucess!");
    	consumer.close();
    }
}
