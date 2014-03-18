package com.sohu.adrd.kafka2hdfs.kafka;

public class FailedFetchException extends RuntimeException{
	private static final long serialVersionUID = 7173834280091207248L;

	public FailedFetchException(String message) {
	        super(message);
	    }

	    public FailedFetchException(Exception e) {
	        super(e);
	    }
}
