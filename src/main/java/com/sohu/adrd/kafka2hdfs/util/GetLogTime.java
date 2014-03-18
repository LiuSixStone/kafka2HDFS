package com.sohu.adrd.kafka2hdfs.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Log format: 2014-03-10 11:31:50 INF {....}
 * 
 * @author leileiliu202357
 *
 */
public class GetLogTime {
	private static final Logger LOG = LoggerFactory.getLogger(GetLogTime.class);
	
	public String parseDate(String log) {
		if(log == null) {
			LOG.error("Log is null, no parsing");
			return null;
		}
		
		int index = log.indexOf("{");
		if(index == -1){
			LOG.error("CountInfo format error:" + log);
			return null;
		}
		
		String[] segs = log.substring(0, index).trim().split("\\s");
		if (segs.length > 0) {
			LOG.info("log date info: " + segs[0]);
			return new String(segs[0].replace('-', '/'));
		}
		
		return null;
	}
	
	public static void main(String[] args) {
		GetLogTime timerCheck = new GetLogTime();
		String dateString = timerCheck.parseDate("2014-03-10 11:24:26 INF {");
		System.out.println(dateString);
	}
}
