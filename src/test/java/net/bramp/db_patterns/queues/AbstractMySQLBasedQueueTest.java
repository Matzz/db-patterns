package net.bramp.db_patterns.queues;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import net.bramp.db_patterns.DatabaseUtils;
import net.bramp.db_patterns.queues.StatusableQueue.ValueWithMetadata;
import net.bramp.serializator.DefaultSerializator;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.base.Function;

@RunWith(Parameterized.class)
public class AbstractMySQLBasedQueueTest {

	private Function<String, Object> valueFactory;
	private AbstractMySQLQueue<Object> queue;

	final static long WAIT_FOR_TIMING_TEST = 300; // in ms

	@Parameterized.Parameters
	public static Collection<Object[]> provideValueFactoriesAndQueues() {
		String queueTableName = "queue";
		DataSource ds = DatabaseUtils.createDataSource();
		String me = "test";
		String queueName = java.util.UUID.randomUUID().toString();
		Function<String, DelayedString> dsFactory = new Function<String, DelayedString>() {
			@Override
			public DelayedString apply(String input) {
				return new DelayedString(input, 0);
			}
		};
		Function<String, String> sFactory = new Function<String, String>() {
			@Override
			public String apply(String input) {
				return input;
			}
		};

		Object delayedQueue = new MySQLBasedDelayQueue<DelayedString>(ds,
				queueTableName, queueName,
				new DefaultSerializator<DelayedString>(), me);
		Object standardQueue = new MySQLBasedQueue<String>(ds, queueTableName,
				queueName, String.class, me);
		return Arrays.asList(new Object[][] {
				new Object[] { dsFactory, delayedQueue },
				new Object[] { sFactory, standardQueue }
				});
	}

	public AbstractMySQLBasedQueueTest(Function<String, Object> valueFactory,
			AbstractMySQLQueue<Object> queue) {
		this.valueFactory = valueFactory;
		this.queue = queue;
	}

	@After
	public void cleanupDatabase() throws SQLException {
		queue.clear();
		queue.cleanupAll(10);
		assertEmpty();
	}

	protected void assertEmpty() {
		assertTrue("Queue should start empty", queue.isEmpty());
		assertEquals("Queue should start empty", 0, queue.size());
		assertNull("Queue head should be null", queue.peek());
	}

	@Test
	public void test() {
		assertEmpty();

		Object a = valueFactory.apply("A");
		assertTrue(queue.add(a));

		assertEquals("Queue should contain one item", 1, queue.size());
		assertEquals("Queue head should be A", a, queue.peek());

		Object b = valueFactory.apply("B");
		assertTrue(queue.add(b));

		assertEquals("Queue should start empty", 2, queue.size());
		
		assertEquals("Queue head should be A", a, queue.peek());
		assertEquals("Queue head should be A", a, queue.poll());

		assertEquals("Queue should start empty", 1, queue.size());
		assertEquals("Queue head should be B", b, queue.peek());

		assertEquals("Queue head should be B", b, queue.poll());

		assertEmpty();
	}

	@Test
	public void statusTest() {
		assertEmpty();

		Object a = valueFactory.apply("A");
		assertTrue(queue.add(a));
		ValueWithMetadata<Object> v = queue.peekWithMetadata();
		queue.updateStatus(v.id, "Test1");
		assertEquals(queue.getStatus(v.id), "Test1");
		queue.updateStatus(v.id, "Test2");
		assertEquals(queue.getStatus(v.id), "Test2");
	}

	/*
	 * TODO We should change this to measure if take actually blocked forever
	 * 
	 * @Test(timeout=5000) public void takeBlockingTest() throws
	 * InterruptedException { assertEmpty();
	 * 
	 * // This should block forever queue.take();
	 * 
	 * assertEmpty(); }
	 */

	@Test(timeout = 1000)
	public void pollBlockingTest() throws InterruptedException {
		assertEmpty();

		long wait = WAIT_FOR_TIMING_TEST;

		long now = System.currentTimeMillis();
		Object ret = queue.poll(wait, TimeUnit.MILLISECONDS);
		long duration = System.currentTimeMillis() - now;

		assertNull("poll timed out", ret);

		assertTrue("We waited less than " + wait + "ms (actual:" + duration
				+ ")", duration >= wait);
		assertTrue("We waited more than " + (wait * 1.2) + "ms (actual:"
				+ duration + ")", duration < wait * 1.2);

		assertEmpty();
	}
}
