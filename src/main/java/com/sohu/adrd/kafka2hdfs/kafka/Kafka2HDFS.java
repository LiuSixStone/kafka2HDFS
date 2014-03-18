package com.sohu.adrd.kafka2hdfs.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sohu.adrd.kafka2hdfs.util.XmlParser;

/**
 * poll data from kafka and push to hdfs
 * 
 * @author leileiliu202357
 *
 */
public class Kafka2HDFS {
	
	public static final Logger LOG = LoggerFactory.getLogger(Kafka2HDFS.class);
	private String topic;
	private int partitionNum;
	private String kafkaZkStr;
	private String offsetZkStr;
	private String offsetZkRoot;
	private String name;
	private List<PartitionAndOffset> offsetList;
	private String rootDirOutput;
	private int fileSize;
	private String compressor;
	private String brokerPath;
	private int fetchInterval;
	
	private KafkaConfig kafkaConfig;
	private Kafka2hdfsConfig kafka2hdfsConfig;
	private ZkHosts zkHosts;
	private ZkBrokerReader zkBrokerReader;
	private ZkState state;
	private DynamicPartitionConnections connections;
	private ExecutorService executor;
	private List<Future<?>> futerList;
	
	private long startTime;
	private long endtime;
	
	public Kafka2HDFS() {
		offsetList = new ArrayList<PartitionAndOffset>();
		futerList = new ArrayList<Future<?>>();
	}
	
	public class PartitionAndOffset {
		public int id;
		public long startOffset = -1;
		public long endOffset = -1;
		
		public PartitionAndOffset(int id, long startOffset, long endOffset) {
			this.id = id;
			this.startOffset = startOffset;
			this.endOffset = endOffset;
		}
		
		@Override
		public String toString() {
			return "[" + id + ":"+ startOffset 
					+ ":" + endOffset + "]";
		}
	}
	
	public void prepare() {
		//parser xml 
		getConfig();
		StringBuilder sb = new StringBuilder();
		sb.append("topic: ").append(topic).append(" partitionNum: ").append(partitionNum).append(" apName: ").append(" name").append("\n");
		sb.append("kafkaZkStr: ").append(kafkaZkStr).append(" brokerPath: ").append(brokerPath).append("\n");
		sb.append("offsetZkStr: ").append(offsetZkStr).append(" offsetZkRoot: ").append(offsetZkRoot).append("\n");
		sb.append("rootDirOutput: ").append(rootDirOutput).append(" fileSize: ").append(fileSize).append(" compressor: ").append(compressor).append("\n");
		sb.append("fetchInterval: ").append(fetchInterval);
		LOG.info("get config :" + sb.toString() + "offset: " + offsetList.toString());
		
		init();
		LOG.info("init sucess!!");
	}
	
	private void getConfig() {
		XmlParser config = new XmlParser();
		try {
			config.parse("config.xml");
		} catch (Exception e) {
			e.printStackTrace();
		}
		Properties props = config.getProps();
		topic = props.getProperty("topic");
		partitionNum = Integer.valueOf(props.getProperty("partitonnumber"));
		kafkaZkStr = props.getProperty("kafkazkstr");
		offsetZkStr = props.getProperty("offsetzkstr");
		offsetZkRoot = props.getProperty("offsetzkroot");
		name = props.getProperty("name");
		rootDirOutput = props.getProperty("rootdir");
		fileSize = Integer.valueOf(props.getProperty("filesize"));
		compressor = props.getProperty("compressor");
		brokerPath = props.getProperty("brokerpath");
		String[] segs = props.getProperty("partitionsandoffsets").trim().split(";");
		for (String seg : segs) {
			String[] offset = seg.trim().split(":");
			PartitionAndOffset offsetRange = new PartitionAndOffset(Integer.valueOf(offset[0]),
					Long.valueOf(offset[1]), Long.valueOf(offset[2]));
			offsetList.add(offsetRange);
		}
		fetchInterval = Integer.valueOf(props.getProperty("fetchinterval"));
	}
	
	private void init() {
		state = new ZkState(offsetZkStr);
		zkHosts = new ZkHosts(kafkaZkStr, brokerPath);
		kafkaConfig = new KafkaConfig(zkHosts, topic);
		String outPathWithtopic = rootDirOutput + "/" + topic;
		kafka2hdfsConfig = new Kafka2hdfsConfig(zkHosts, topic, offsetZkRoot, name, 
				outPathWithtopic, fileSize, compressor, offsetList);
		zkBrokerReader = new ZkBrokerReader(topic, zkHosts);
		connections = new DynamicPartitionConnections(kafkaConfig, zkBrokerReader);
		executor = Executors.newFixedThreadPool(partitionNum);//one partition one thread
	}
	
	private void run() {
		for (int i = 0; i < partitionNum; i++) {
			//for (int i = 0; i < 2; i++) {
			Kafka2HDFSTask task = new Kafka2HDFSTask(connections, kafka2hdfsConfig, state, i, partitionNum, fetchInterval);
			Future<?> retFuture = executor.submit(task);
			futerList.add(retFuture);
		}
		LOG.info("create threads sucess!!");

		startTime = System.currentTimeMillis();
	}
	
	private void clean() {
		executor.shutdown();
		LOG.info("shut down thread pool");
		
		try {
			while (true) {		
				boolean flag = true;
				
				Thread.sleep(60*1000);
				
				for (Future<?> index : futerList) {
					if (!index.isDone()) {
						flag = false;
						LOG.info("task has not completed!!");
						break;//not complete
					}
				}

				if (flag) {
					LOG.info("task has completed!!");
					endtime = System.currentTimeMillis();
					LOG.info("task use time: " + (endtime-startTime));
					break;//has completed
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			//do cleanUp
			connections.clear();
			zkBrokerReader.close();
			state.close();
			LOG.info("clean up!!");
		}
	}
	

	public static void main(String[] args) {
    	PropertyConfigurator.configure("log4j.properties");
		Kafka2HDFS task = new Kafka2HDFS();
		task.prepare();
		task.run();
		task.clean();
	}
}
