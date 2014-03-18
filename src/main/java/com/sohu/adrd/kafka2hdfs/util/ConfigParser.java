package com.sohu.adrd.kafka2hdfs.util;

import java.util.Properties;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class ConfigParser extends DefaultHandler{
	
	private Properties props;
	private String currentName;
	private StringBuffer currentValue = new StringBuffer();
	
	public ConfigParser() {
		this.props = new Properties();
	}
	
	public Properties getProps() {
		return this.props;
	}
	
	public void characters(char[] ch, int start, int length) throws SAXException {
		currentValue.append(ch, start, length);
	}
	
	public void endElement(String uri, String localName, String name) throws SAXException {
		props.put(currentName, currentValue.toString().trim());
	}
	
	public void startElement(String uri, String localName, String qName, Attributes attributes)
	throws SAXException {
		currentValue.delete(0, currentValue.length());
		currentName = qName;
	}
}
