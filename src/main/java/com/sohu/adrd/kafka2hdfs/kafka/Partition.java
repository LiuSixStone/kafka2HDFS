package com.sohu.adrd.kafka2hdfs.kafka;

import com.google.common.base.Objects;

public class Partition {
	
	public final Broker host;
	public final int partition;

	public Partition(Broker host, int partition) {
		this.host = host;
		this.partition = partition;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(host, partition);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		final Partition other = (Partition) obj;
		return Objects.equal(this.host, other.host) && Objects.equal(this.partition, other.partition);
	}

	@Override
	public String toString() {
		return "Partition{" +
				"host=" + host +
				", partition=" + partition +
				'}';
	}

	public String getId() {
		return "partition_" + partition;
	}

}
