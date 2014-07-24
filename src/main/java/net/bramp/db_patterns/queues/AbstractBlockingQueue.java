package net.bramp.db_patterns.queues;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * To keep code neat, most of the simple methods are here
 * @author bramp
 */
abstract class AbstractBlockingQueue<E> implements BlockingQueue<E> {

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean offer(E e) {
		return add(e);
	}
	
	@Override
	public E element() {
		E head = peek();
		if (head == null)
			throw new NoSuchElementException();
		return head;
	}

	@Override
	public E remove() {
		E head = poll();
		if (head == null)
			throw new NoSuchElementException();
		return head;
	}


	/**
	 * Blocks
	 */
	@Override
	public void put(E e) throws InterruptedException {
		add(e);
	}
	
	/**
	 * No blocking
	 */
	@Override
	public int drainTo(Collection<? super E> c) {
		return drainTo(c, Integer.MAX_VALUE);
	}

	/**
	 * No blocking
	 */
	@Override
	public int drainTo(Collection<? super E> c, int maxElements) {
		if (c == this)
			throw new IllegalArgumentException("Draining to self is not supported");

		int count = 0;
		while (count < maxElements) {
			E head = poll();
			if (head == null)
				break;

			c.add(head);
			count++;
		}

		return maxElements - count;
	}
	
	@Override
	public int remainingCapacity() {
		return Integer.MAX_VALUE;
	}

	@Override
	public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
		// Right now, we have no concept of a full queue, so we don't block on insert
		return offer(e);
	}

	////// Nothing supported below

	@Override
	public boolean contains(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<E> iterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] toArray() {
		throw new UnsupportedOperationException();	}

	@Override
	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}
}
