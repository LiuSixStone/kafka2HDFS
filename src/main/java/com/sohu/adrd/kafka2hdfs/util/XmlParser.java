package com.sohu.adrd.kafka2hdfs.util;

import java.util.Properties;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

public class XmlParser {
	
	private Properties props;
	
	public Properties getProps() {
		return this.props;
	}
	
	public void parse(String fileName) throws Exception {
		ConfigParser handler = new ConfigParser();
		
		SAXParserFactory factory = SAXParserFactory.newInstance();
		factory.setNamespaceAware(false);
		factory.setValidating(false);
		
		SAXParser parser = factory.newSAXParser();
		try {
			parser.parse(fileName, handler);
			props=handler.getProps();
		} finally {
			handler = null;
			parser = null;
			factory = null;
		}
	}
	
	
	
	public static void main(String[] args) {
		System.out.println(System.getProperty("user.dir"));
		
		XmlParser testParser = new XmlParser();
		try {
			testParser.parse("src\\main\\resources\\config.xml");
			Properties props = new Properties();
			props = testParser.getProps();
			
			System.out.println(props.getProperty("topic"));
			System.out.println(props.getProperty("kafkazkstr"));
			System.out.println(props.getProperty("offsetzkstr"));
			System.out.println(props.getProperty("offsetzkroot"));
			System.out.println(props.getProperty("name"));
			System.out.println(props.getProperty("partitionsandoffsets"));
			
			
			System.out.println(props.getProperty("rootdir"));
			System.out.println(props.getProperty("filesize"));
			System.out.println(props.getProperty("compressor"));
			
			} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	
	}
	
}
