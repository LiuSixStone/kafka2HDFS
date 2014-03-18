package com.sohu.adrd.kafka2hdfs.kafka;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.sohu.adrd.kafka2hdfs.util.ConstData;

public class DynamicBrokersReader {
	public static final Logger LOG = LoggerFactory.getLogger(DynamicBrokersReader.class);

	private CuratorFramework curator;
	private String zkPath;
	private String topic;

	public DynamicBrokersReader(String zkStr, String zkPath, String topic) {
		this.zkPath = zkPath;
		this.topic = topic;
		
		try {
			curator = CuratorFrameworkFactory.newClient(
					zkStr, ConstData.SESSION_TIMEOUT, 15000,
					new RetryNTimes((ConstData.RETRY_TIMES), ConstData.RETRY_INTERVAL));
			
			curator.start();
		} catch (IOException ex) {
			LOG.error("can't connect to zookeeper");
		}
	}

	
	 /**
     * Get all partitions with their current leaders
     */
    public GlobalPartitionInformation getBrokerInfo() {
        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
        try {
            int numPartitionsForTopic = getNumPartitions();
            String brokerInfoPath = brokerPath();
            for (int partition = 0; partition < numPartitionsForTopic; partition++) {
                int leader = getLeaderFor(partition);
                String path = brokerInfoPath + "/" + leader;
                try {
                    byte[] brokerData = curator.getData().forPath(path);
                    Broker hp = getBrokerHost(brokerData);
                    globalPartitionInformation.addPartition(partition, hp);
                } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
                    LOG.error("Node {} does not exist ", path);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOG.info("Read partition info from zookeeper: " + globalPartitionInformation);
        return globalPartitionInformation;
    }
    
    private int getNumPartitions() {
        try {
            String topicBrokersPath = partitionPath();
            List<String> children = curator.getChildren().forPath(topicBrokersPath);
            return children.size();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String partitionPath() {
        return zkPath + "/topics/" + topic + "/partitions";
    }

    public String brokerPath() {
        return zkPath + "/ids";
    }

    /**
     * get /brokers/topics/distributedTopic/partitions/1/state
     * { "controller_epoch":4, "isr":[ 1, 0 ], "leader":1, "leader_epoch":1, "version":1 }
     *
     * @param partition
     * @return
     */
    private int getLeaderFor(long partition) {
        try {
            String topicBrokersPath = partitionPath();
            byte[] hostPortData = curator.getData().forPath(topicBrokersPath + "/" + partition + "/state");
            Map<Object, Object> value = (Map<Object, Object>) JSONValue.parse(new String(hostPortData, "UTF-8"));
            Integer leader = ((Number) value.get("leader")).intValue();
            return leader;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        curator.close();
        curator = null;
    }

    /**
     * [zk: localhost:2181(CONNECTED) 56] get /brokers/ids/0
     * { "host":"localhost", "jmx_port":9999, "port":9092, "version":1 }
     *
     * @param contents
     * @return
     */
    private Broker getBrokerHost(byte[] contents) {
        try {
            Map<Object, Object> value = (Map<Object, Object>) JSONValue.parse(new String(contents, "UTF-8"));
            String host = (String) value.get("host");
            Integer port = ((Long) value.get("port")).intValue();
            return new Broker(host, port);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
