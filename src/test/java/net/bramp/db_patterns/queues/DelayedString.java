package net.bramp.db_patterns.queues;

import java.io.Serializable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;


public class DelayedString implements Comparable<Delayed>, Delayed, Serializable {

	private static final long serialVersionUID = -574306132564575817L;
	
	private String str;
	private long time;
	private transient TimeUnit unit = TimeUnit.SECONDS;
	
	public DelayedString(String str, long seconds) {
		this.str = str;
		this.time = seconds + nowInSeconds();
	}
	
	public String get() {
		return str;
	}

	@Override
	public int compareTo(Delayed o) {
		Long l = o.getDelay(unit);
		return l.compareTo(this.time);
	}

	
	@Override
	public long getDelay(TimeUnit unit) {
		return unit.convert(time - nowInSeconds(), unit);
	}

	@Override
	public boolean equals(Object o) {
		if(o instanceof DelayedString) {
			DelayedString v = (DelayedString) o;
			return get().equals(v.get());
		}
		else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return get().hashCode();
	}

	@Override
	public String toString() {
		return get();
	}
	
	private long nowInSeconds() {
		return System.currentTimeMillis()/1000;
	}
}
