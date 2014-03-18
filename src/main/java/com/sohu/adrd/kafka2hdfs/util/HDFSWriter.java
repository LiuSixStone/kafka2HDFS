package com.sohu.adrd.kafka2hdfs.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * filePath: hdfs://ip:port/user/adrd/kafka2hadoop/countinfo/2014/03/12/partiton_XXX-partXXX.lzo
 * 
 * 
 * @author leileiliu202357
 *
 */
public class HDFSWriter {
	private static final Logger LOG = LoggerFactory.getLogger(HDFSWriter.class);
	
	private final String rootPath;
	private final String compressor;
	private final long fileSize;
	
	private long counter;
	//private int partitionID;
	private String date = null;
	private int fileIndex = 0;
	private final String fileSuffix;
	private CompressionCodec codec;	
	private CompressionOutputStream output;
	private Path path;
	
	private static Configuration conf;
	private FileSystem fs;
	static {
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://10.31.72.58:8020");
	}


	public HDFSWriter(String rootPath, int fileSize, String compressor, int partitionID) {
		this.fileSize = fileSize * 1024 *1024;
		this.rootPath = rootPath;
		//this.partitionID = partitionID;
		this.compressor = compressor;
		fileSuffix = "/partition_" + partitionID + "-part";
		
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			LOG.error("get hdfs FileSystem error: " + e.getMessage());
			e.printStackTrace();
		}
		makeCompressor(compressor);
		resetCounter();
	}
	
	public boolean Writer(String messages, String messageDate) {
		if (!checkAndCreatefile(messageDate)) return false;
		if (!write(messages)) return false;
		if (!checkAndCreatePart()) return false;
		return true;
	}
	
	public void close() {
		try {
			output.close();
			fs.close();
		} catch (Exception e) {
			LOG.error("close fs error: " + e.getMessage());
		}
		LOG.info("close fs sucess!!");
	}

	private boolean checkAndCreatePart() {
		counter--;
		if (counter <= 0) {

			try {
				FileStatus status = fs.getFileLinkStatus(path);
				if (status.getLen() >= fileSize) {
					fileIndex++;
					String file = getFileName(date);
					output.close();
					path = new Path(file);
					output = codec.createOutputStream(fs.create(path));
					resetCounter();
				}
			} catch (Exception e) {
				LOG.error("get filestatus error: " + e.getMessage());
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}

	private void resetCounter() {
		counter = 100;
	}
	
	private boolean write(String messages) {
		try {
			output.write(messages.getBytes());
		} catch (IOException e) {
			LOG.error("write 2 hdfs error! for: " + e.getMessage());
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	private void makeCompressor(String type) {
		String codecClassString;
		if (type.equalsIgnoreCase("lzo")) {
			codecClassString  = "com.hadoop.compression.lzo.LzopCodec";
		}else if (type.equalsIgnoreCase("snappy")) {
			codecClassString = "org.apache.hadoop.io.compress.SnappyCodec";
		}else if (type.equalsIgnoreCase("zip")) {
			codecClassString = "org.apache.hadoop.io.compress.GzipCodec";
		}else {
			codecClassString = "org.apache.hadoop.io.compress.DefaultCodec";
		}
		Class<?> codecClass = null;
		try {
			codecClass = Class.forName(codecClassString);
		} catch (ClassNotFoundException e) {
			LOG.error("get CompressionCodec error: " + e.getMessage());
			e.printStackTrace();
		}
		codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
	}
	
	private boolean checkAndCreatefile(String messageDate) {
		if (date == null || !date.endsWith(messageDate)) {
			//reset fileIndex
			fileIndex = 0;
			String file = getFileName(messageDate);
			
			try {
				if (date != null) {
					output.close();
				}
				path = new Path(file);
				output = codec.createOutputStream(fs.create(path));
			} catch (Exception e) {
				LOG.error("create new file for new day error: " + e.getMessage());
				e.printStackTrace();
				return false;
			}
			date = messageDate;
			resetCounter();
			
			LOG.info("create new hdfs file: " + file);
		}
		return true;
	}
	
	private String getFileName(String messageDate) {
		StringBuilder sb = new StringBuilder();
		sb.append(rootPath).append("/").append(messageDate).append(fileSuffix).append(fileIndex);
		sb.append(".").append(compressor);
		return sb.toString();
	}
	
	public static void main(String[] args) {
		String message = "this is for test compressor!!\nhdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txt"
				+ "hdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txt\n"
				+ "hdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txt\n"
				+ "hdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txt\n"
				+ "hdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txthdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txt"
				+ "hdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txt\n"
				+ "hdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txthdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txt\n"
				+ "hdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txt";
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.31.72.58:8020");
		//conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		//conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			Path p = new Path("hdfs://10.31.72.58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/partition_0-part0.lzo");
			FileStatus status = fs.getFileStatus(p);
			System.out.println("file lenth = " + status.getLen());
			
			/*fs.createNewFile(p);	
			Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.SnappyCodec");
			CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
			
			FSDataOutputStream output = fs.create(p);
			output.writeUTF(message);
			output.close();*/
			//CompressionOutputStream output = codec.createOutputStream(fs.create(p));
			//output.write(message.getBytes());
			//output.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				
				fs.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}
	
}
