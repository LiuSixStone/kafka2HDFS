package com.sohu.adrd.kafka2hdfs.kafka;

public class ZkHosts {
	private static final String DEFAULT_ZK_PATH = "/brokers";
	
	public String brokerZkStr = null;
	public String brokerZkPath = null;
	public int refreshFreqSecs = 60;
	
	public ZkHosts(String brokerZkStr, String brokerZkPath) {
		this.brokerZkStr = brokerZkStr;
		this.brokerZkPath = brokerZkPath;
	}
	
	public ZkHosts(String brokerZkStr) {
		this(brokerZkStr, DEFAULT_ZK_PATH);
	}
}
