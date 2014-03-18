package com.sohu.adrd.kafka2hdfs.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkBrokerReader {
	public static final Logger LOG = LoggerFactory.getLogger(ZkBrokerReader.class);
	
	GlobalPartitionInformation cachedBrokers;
    DynamicBrokersReader reader;
    long lastRefreshTimeMs;
    long refreshMillis;
    
    public ZkBrokerReader(String topic, ZkHosts hosts) {
        reader = new DynamicBrokersReader(hosts.brokerZkStr, hosts.brokerZkPath, topic);
        cachedBrokers = reader.getBrokerInfo();
        lastRefreshTimeMs = System.currentTimeMillis();
        refreshMillis = hosts.refreshFreqSecs * 1000L;
    }
    
    public GlobalPartitionInformation getCurrentBrokers() {
        long currTime = System.currentTimeMillis();
        if (currTime > lastRefreshTimeMs + refreshMillis) {
            LOG.info("brokers need refreshing because " + refreshMillis + "ms have expired");
            cachedBrokers = reader.getBrokerInfo();
            lastRefreshTimeMs = currTime;
        }
        return cachedBrokers;
    }
    
    public void close() {
        reader.close();
        reader = null;
    }
}
