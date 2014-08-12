package net.bramp.db_patterns.queues;

import net.bramp.db_patterns.queues.interfaces.ValueWithMetadata;
import net.bramp.db_patterns.queues.interfaces.ValueWithPriority;

public class ValueContainer<E> implements ValueWithMetadata<E>, ValueWithPriority<E> {

	public static final int DEFAULT_PRIORRITY = 0;
	
	protected long id;
	protected String status;
	protected long priority;
	protected E value;

	ValueContainer(long id, String status, long priority, E value) {
		this.id = id;
		this.status = status;
		this.priority = priority;
		this.value = value;
	}
	ValueContainer(long id, String status, E value) {
		this(id, status, DEFAULT_PRIORRITY, value);
	}
	@Override
	public long getId() {
		return id;
	}
	@Override
	public String getStatus() {
		return status;
	}
	@Override
	public long getPriority() {
		return priority;
	}
	@Override
	public E getValue() {
		return value;
	}
	@Override
	public String toString() {
		return "ValueWithMetadata["+id+" "+status+" "+value+"]";
	}
}