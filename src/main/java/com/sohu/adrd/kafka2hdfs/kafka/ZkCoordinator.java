package com.sohu.adrd.kafka2hdfs.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkCoordinator {
	public static final Logger LOG = LoggerFactory.getLogger(ZkCoordinator.class);

	private Kafka2hdfsConfig kafka2hdfsConfig;
	private int taskIndex;
	private int totalTasks;
	private Map<Partition, PartitionManager> managers = new HashMap();
	private List<PartitionManager> cachedList;
	private Long lastRefreshTime;
	private int refreshFreqMs;
	private DynamicPartitionConnections connections;
	private DynamicBrokersReader reader;
	private ZkState state;
	
	public ZkCoordinator(DynamicPartitionConnections connections, Kafka2hdfsConfig kafka2hdfsConfig, ZkState state, int taskIndex, int totalTasks) {
		this.kafka2hdfsConfig = kafka2hdfsConfig;
		this.connections = connections;
		this.taskIndex = taskIndex;
		this.totalTasks = totalTasks;
		this.state = state;

		ZkHosts brokerConf = kafka2hdfsConfig.hosts;
		this.refreshFreqMs = brokerConf.refreshFreqSecs * 1000;
		this.reader = new DynamicBrokersReader(brokerConf.brokerZkStr, brokerConf.brokerZkPath, kafka2hdfsConfig.topic);

	}

    public List<PartitionManager> getMyManagedPartitions() {
        if (lastRefreshTime == null || (System.currentTimeMillis() - lastRefreshTime) > refreshFreqMs) {
            refresh();
            lastRefreshTime = System.currentTimeMillis();
        }
        return cachedList;
    }
    
    void refresh() {
        try {
            LOG.info("Refreshing partition manager connections");
            GlobalPartitionInformation brokerInfo = reader.getBrokerInfo();
            Set<Partition> mine = new HashSet();
            
            List<Partition> partitions = brokerInfo.getOrderedPartitions();
            for(int i=taskIndex; i<partitions.size(); i+=totalTasks) {
                Partition myPartition = partitions.get(i);
                mine.add(myPartition);
            }

            Set<Partition> curr = managers.keySet();
            Set<Partition> newPartitions = new HashSet<Partition>(mine);
            newPartitions.removeAll(curr);

            Set<Partition> deletedPartitions = new HashSet<Partition>(curr);
            deletedPartitions.removeAll(mine);

            LOG.info("Deleted partition managers: " + deletedPartitions.toString());

            for (Partition id : deletedPartitions) {
                PartitionManager man = managers.remove(id);
                man.close();
            }
            LOG.info("New partition managers: " + newPartitions.toString());

            for (Partition id : newPartitions) {
                PartitionManager man = new PartitionManager(connections, state, kafka2hdfsConfig, id);
                managers.put(id, man);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        cachedList = new ArrayList<PartitionManager>(managers.values());
        LOG.info("Finished refreshing");
    }
    
    public PartitionManager getManager(Partition partition) {
        return managers.get(partition);
    }
}
