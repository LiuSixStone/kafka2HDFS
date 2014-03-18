package com.sohu.adrd.kafka2hdfs.kafka;

public class KafkaConfig {

	public final ZkHosts hosts;
	public final String topic;
	public final String clientId;
	
	public int fetchSizeBytes = 1024 * 1024;
	public int socketTimeoutMs = 10000;
	public int bufferSizeBytes = 1024 * 1024;
	public boolean forceFromStart = false;
	public long startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
	public boolean useStartOffsetTimeIfOffsetOutOfRange = true;
	
	public KafkaConfig(ZkHosts hosts, String topic) {
		this(hosts, topic, kafka.api.OffsetRequest.DefaultClientId());
	}
	
	public KafkaConfig(ZkHosts hosts, String topic, String clientId) {
		this.hosts = hosts;
		this.topic = topic;
		this.clientId = clientId;
	}
}
