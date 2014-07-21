package net.bramp.db_patterns.queues;

import java.util.concurrent.TimeUnit;

public interface StatusableQueue<E> {
	public ValueWithMetadata<E> pollWithMetadata();
	public ValueWithMetadata<E> pollWithMetadata(long timeout, TimeUnit unit) throws InterruptedException;
	public ValueWithMetadata<E> peekWithMetadata();
	public void updateStatus(long id, String newStatus);
	public String getStatus(long id);
	
	public static class ValueWithMetadata<E> {
		public final long id;
		public final String status;
		public final E value;
		ValueWithMetadata(long id, String status, E value) {
			this.id = id;
			this.status = status;
			this.value = value;
		}
		@Override
		public String toString() {
			return "ValueWithMetadata["+id+" "+status+" "+value+"]";
		}
	}
}
