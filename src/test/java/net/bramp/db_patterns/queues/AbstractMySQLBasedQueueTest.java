package net.bramp.db_patterns.queues;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import net.bramp.db_patterns.DatabaseUtils;
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
				new Object[] { sFactory, standardQueue } });
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
		ValueContainer<Object> v = queue.peekWithMetadata();
		queue.updateStatus(v.getId(), "Test1");
		assertEquals(queue.getStatus(v.id), "Test1");
		queue.updateStatus(v.getId(), "Test2");
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

	@Test
	public void priorityTest() {
		assertEmpty();

		Object a = valueFactory.apply("A");
		Object b = valueFactory.apply("B");
		Object c = valueFactory.apply("C");
		Object d = valueFactory.apply("D");
		assertTrue(queue.add(a, 1));
		assertTrue(queue.add(b, 0));
		assertTrue(queue.add(c, 2));
		assertTrue(queue.add(d, 3));

		assertEquals("Queue head should be D", d, queue.peek());
		assertEquals("Queue head should be D", d, queue.poll());
		assertEquals("Queue head should be C", c, queue.poll());
		assertEquals("Queue head should be A", a, queue.poll());
		assertEquals("Queue head should be B", b, queue.poll());

		assertEmpty();
	}

	@Test
	public void priorityRandomTest() {
		assertEmpty();

		Random r = new Random();
		for (int i = 0; i < 100; i++) {
			Object s = valueFactory.apply("Test str");
			assertTrue(queue.add(s, r.nextInt()));
		}

		long prevPriority = Long.MAX_VALUE;
		ValueContainer<Object> vc;
		while ((vc = queue.pollWithMetadata()) != null) {
			assertTrue("Next priority should be <= previous",
					vc.getPriority() <= prevPriority);
		}
	}

	@Test
	public void getPriorityTest() {
		assertEmpty();

		queue.add(valueFactory.apply("a"), 11);
		queue.add(valueFactory.apply("b"), 10);
		queue.add(valueFactory.apply("c"), 12);
		assertTrue(queue.pollWithMetadata().priority == 12);
		assertTrue(queue.pollWithMetadata().priority == 11);
		assertTrue(queue.pollWithMetadata().priority == 10);

	}

	@Test
	public void multhreadUniqResultsTest() throws InterruptedException,
			ExecutionException {
		assertEmpty();

		class Worker implements Callable<List<Object>> {
			@Override
			public List<Object> call() {
				List<Object> done = new LinkedList<Object>();
				try {
					Object last = null;
					do {
						last = queue.poll(5, TimeUnit.SECONDS);
						if(last!=null) {
							done.add(last);
						}
					} while (last != null);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				return done;
			}

		}

		int threadsCnt = 20;
		int itemsCnt = 500;
		
		for (int i = 0; i < itemsCnt; i++) {
			assertTrue(queue.add(valueFactory.apply(String.valueOf(i))));
		}
		ExecutorService executor = Executors.newFixedThreadPool(threadsCnt);
		List<Future<List<Object>>> futures = new ArrayList<Future<List<Object>>>(
				threadsCnt);
		for (int i = 0; i < threadsCnt; i++) {
			futures.add(executor.submit(new Worker()));
		}
		List<Object> allDoneList = new LinkedList<Object>();
		for (int i = 0; i < threadsCnt; i++) {
			List<Object> currentDone = futures.get(i).get();
			System.out.println(currentDone);
			allDoneList.addAll(currentDone);
		}
		Set<Object> uniqSet = new HashSet<Object>(allDoneList);
		assertEquals(uniqSet.size(), allDoneList.size());
		assertEmpty();
	}
}
