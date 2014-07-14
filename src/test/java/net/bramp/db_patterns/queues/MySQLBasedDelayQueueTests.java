package net.bramp.db_patterns.queues;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import net.bramp.db_patterns.DatabaseUtils;
import net.bramp.serializator.DefaultSerializator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MySQLBasedDelayQueueTests {

	final static long WAIT_FOR_TIMING_TEST = 300; // in ms
	
	private String queueName;
	private DataSource ds;
	
	private MySQLBasedQueue<DelayedString> queue;

	@Before
	public void setup() {
		// Different queue name for each test (to avoid test clashes)
		queueName = java.util.UUID.randomUUID().toString();
		ds = DatabaseUtils.createDataSource();

		queue = new MySQLBasedDelayQueue<DelayedString>(ds, queueName, new DefaultSerializator<DelayedString>(), "test");
	}

	@After
	public void cleanupDatabase() throws SQLException {
		queue.clear();
		queue.cleanupAll();
		assertEmpty();
	}
	
	protected void assertEmpty() {
		assertTrue("Queue should start empty", queue.isEmpty());
		assertEquals("Queue should start empty", 0, queue.size());
		assertNull("Queue head should be null", queue.peek());
	}
	
	@Test
	public void test() throws IOException {
		
		assertEmpty();

		DelayedString a = new DelayedString("A", 0);
		DelayedString b = new DelayedString("B", 0);
		
		assertTrue( queue.add(new DelayedString("A", 0)) );

		assertEquals("Queue should contain one item", 1, queue.size());
		assertEquals("Queue head should be A", a, queue.peek());

		assertTrue( queue.add(b) );

		assertEquals("Queue should start empty", 2, queue.size());
		assertEquals("Queue head should be A", a, queue.peek());

		assertEquals("Queue head should be A", a, queue.poll());

		assertEquals("Queue should start empty", 1, queue.size());
		assertEquals("Queue head should be B", b, queue.peek());
		
		assertEquals("Queue head should be B", b, queue.poll());

		assertEmpty();
	}
	
	@Test
	public void nonBlockingPeekTest() throws IOException, InterruptedException {
		assertEmpty();
		long s = 2;
		DelayedString a = new DelayedString("A", s);
		assertTrue( queue.add(a) );
		assertEquals("Queue should contain one item", 1, queue.size());
		assertNull("Queue head should be null", queue.peek());
		Thread.sleep(s*2*1000l);
		assertEquals("Queue head should be null", a, queue.peek());
	}

	@Test(timeout=10*1000)
	public void delayedPollBlockingTest() throws IOException, InterruptedException, SQLException {
		assertEmpty();
		long s = 2;
		DelayedString a = new DelayedString("A", s);
		
		assertTrue( queue.add(a) );

		assertNull("Queue head should be null", queue.peek());
		
		Thread.sleep(s*2*1000l);
		DelayedString ds = queue.poll();
		
		assertEquals("Queue head should be null", a, ds);
		assertEmpty();
	}
	
	@Test(timeout=1000)
	public void pollBlockingTest() throws InterruptedException {
		assertEmpty();

		long wait = WAIT_FOR_TIMING_TEST;

		long now = System.currentTimeMillis();
		DelayedString ret = queue.poll(wait, TimeUnit.MILLISECONDS);
		long duration = System.currentTimeMillis() - now;

		assertNull("poll timed out", ret);

		assertTrue("We waited less than " + wait + "ms (actual:" + duration + ")", duration >= wait);
		assertTrue("We waited more than " + (wait*1.2) + "ms (actual:" + duration + ")", duration < wait * 1.2);

		assertEmpty();
	}
	
	protected static class DelayedString implements Delayed, Serializable {

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
		
		private long nowInSeconds() {
			return System.currentTimeMillis()/1000;
		}
	}
}
