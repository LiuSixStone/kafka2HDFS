package com.sohu.adrd.kafka2hdfs.kafka;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sohu.adrd.kafka2hdfs.kafka.PartitionManager.EmitState;

public class Kafka2HDFSTask implements Runnable{
	public static final Logger LOG = LoggerFactory.getLogger(Kafka2HDFSTask.class);
	
	private Kafka2hdfsConfig kafka2hdfsConfig;
	private int taskIndex;
	private int totalTasks;
	private DynamicPartitionConnections connections;
	private ZkState state;
	private int fetchInterval;
	
	private ZkCoordinator zkCoordinator;
	private long lastUpdateMs = 0;
	private int currPartitionIndex = 0;
	
	public Kafka2HDFSTask(DynamicPartitionConnections connections, Kafka2hdfsConfig kafka2hdfsConfig, ZkState state, 
			int taskIndex, int totalTasks, int fetchInterval) {
		this.kafka2hdfsConfig = kafka2hdfsConfig;
		this.connections = connections;
		this.taskIndex = taskIndex;
		this.totalTasks = totalTasks;
		this.state = state;
		this.fetchInterval = fetchInterval;
	}
	
	@Override
	public void run() {
		zkCoordinator = new ZkCoordinator(connections, kafka2hdfsConfig, state, taskIndex, totalTasks);
		
		while (true) {
			if (fetchInterval != 0) {
				try {
					Thread.sleep(fetchInterval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			List<PartitionManager> managers = zkCoordinator.getMyManagedPartitions();
			for (int i = 0; i < managers.size(); i++) {
				// in case the number of managers decreased
				currPartitionIndex = currPartitionIndex % managers.size();
				PartitionManager.EmitState state = managers.get(currPartitionIndex).next();
				
				currPartitionIndex = (currPartitionIndex + 1) % managers.size();
				if (state != EmitState.NO_EMITTED) {
					break;
				}
			}

			long now = System.currentTimeMillis();
			if ((now - lastUpdateMs) > kafka2hdfsConfig.stateUpdateIntervalMs) {
				commit();
			}
			
			boolean allFinished = true;
			for (PartitionManager manager : managers) {
				allFinished &= manager.isDone();
			}
			if (allFinished) {
				commit(); //force commit
				LOG.info("taskIndex :" + taskIndex + " has compeleted the job!");
				break;
			}

		}
		
	}
	
    private void commit() {
        lastUpdateMs = System.currentTimeMillis();
        for (PartitionManager manager : zkCoordinator.getMyManagedPartitions()) {
            manager.commit();
        }
    }
}
