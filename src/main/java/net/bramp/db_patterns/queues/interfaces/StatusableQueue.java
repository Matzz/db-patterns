package net.bramp.db_patterns.queues.interfaces;

import java.util.concurrent.TimeUnit;

public interface StatusableQueue<E, V extends ValueWithMetadata<E>> {
	public V pollWithMetadata();
	public V pollWithMetadata(long timeout, TimeUnit unit) throws InterruptedException;
	public V peekWithMetadata();
	public V takeWithMetadata() throws InterruptedException;
	public void updateStatus(long id, String newStatus);
	public String getStatus(long id);
}
