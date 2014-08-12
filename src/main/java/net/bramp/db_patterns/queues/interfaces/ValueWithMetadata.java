package net.bramp.db_patterns.queues.interfaces;

public interface ValueWithMetadata<E> {
	public long getId();
	public String getStatus();
	public E getValue();
}