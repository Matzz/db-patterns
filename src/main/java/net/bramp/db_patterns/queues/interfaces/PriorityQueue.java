package net.bramp.db_patterns.queues.interfaces;

public interface PriorityQueue<E> {
	public boolean add(E value, int priority);
}
