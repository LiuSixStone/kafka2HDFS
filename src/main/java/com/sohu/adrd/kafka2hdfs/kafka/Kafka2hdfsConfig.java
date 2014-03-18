package com.sohu.adrd.kafka2hdfs.kafka;

import java.util.List;

import com.sohu.adrd.kafka2hdfs.kafka.Kafka2HDFS.PartitionAndOffset;

public class Kafka2hdfsConfig extends KafkaConfig {
	public String zkRoot = null;
	public String id = null;
	public long stateUpdateIntervalMs = 2000;
	
	public String outDir;
	public int fileSize;
	public String compressor;
	public List<PartitionAndOffset> offsetList;

	public Kafka2hdfsConfig(ZkHosts hosts, String topic, String zkRoot, String id, 
			String outDir, int fileSize, String compressor, List offsetList) {
		super(hosts, topic);
		this.zkRoot = zkRoot;
		this.id = id;
		this.outDir = outDir;
		this.fileSize = fileSize;
		this.compressor = compressor;
		this.offsetList = offsetList;
	}
}
