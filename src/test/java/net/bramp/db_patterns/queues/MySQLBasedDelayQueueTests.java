package net.bramp.db_patterns.queues;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import net.bramp.db_patterns.DatabaseUtils;
import net.bramp.serializator.DefaultSerializator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MySQLBasedDelayQueueTests {

	final static long WAIT_FOR_TIMING_TEST = 300; // in ms

	private String queueTable;
	private String queueName;
	private DataSource ds;
	
	private MySQLBasedDelayQueue<DelayedString> queue;

	@Before
	public void setup() {
		// Different queue name for each test (to avoid test clashes)
		queueTable = "queue";
		queueName = java.util.UUID.randomUUID().toString();
		ds = DatabaseUtils.createDataSource();

		queue = new MySQLBasedDelayQueue<DelayedString>(ds, queueTable, queueName, new DefaultSerializator<DelayedString>(), "test");
	}

	@After
	public void cleanupDatabase() throws SQLException {
		queue.clear();
		queue.cleanupAll(0);
		assertEmpty();
		ds.getConnection().close();
	}
	
	protected void assertEmpty() {
		assertTrue("Queue should start empty", queue.isEmpty());
		assertEquals("Queue should start empty", 0, queue.size());
		assertNull("Queue head should be null", queue.peek());
	}
	
	protected void assertBetween(long diff, long min, long max) {
		assertTrue("Invalid range "+min+" <= "+diff+" <= "+max, min <= diff && diff <= max);
	}

	@Test
	public void getClosestDelayTest() throws SQLException {
		long d, queueDelay;
		
		assertEmpty();
		assertEquals(0, queue.getClosestDelay());
		d = 20;
		queue.add(new DelayedString("A", d));
		queueDelay = queue.getClosestDelay();
		assertTrue("Closest delay should be close to added delay ["+d+"<="+(queueDelay+2)+"]", d <= queueDelay+2);
		assertTrue("Closest delay should be less than last added ["+d+"<="+queueDelay+"]", queueDelay <= d);
		
		queue.clear();
		d = 0;
		queue.add(new DelayedString("A", d));
		assertTrue(queue.getClosestDelay()<=d);
	}
	
	
	@Test
	public void nonBlockingPeekTest() throws IOException, InterruptedException {

		assertEmpty();
		long delayS = 2;
		DelayedString a = new DelayedString("A", delayS);
		assertTrue( queue.add(a) );
		assertEquals("Queue should contain one item", 1, queue.size());
		assertNull("Queue head should be null", queue.peek());
		Thread.sleep(delayS*2*1000l);
		assertEquals("Queue head should not be expired task", a, queue.peek());
	}
	

	@Test(timeout=10000)
	public void delayedBlockingPollTest() throws IOException, InterruptedException, SQLException {
		assertEmpty();
		long s = 2;
		DelayedString a = new DelayedString("A", s);

		assertTrue( queue.add(a) );
		assertNull("Queue head should be null", queue.peek());

		DelayedString ds = queue.poll(s*2, TimeUnit.SECONDS);

		assertEquals("Queue head should be object", a, ds);
		assertEmpty();
	}
	

	@Test(timeout=10000)
	public void multiplePoolTest() throws IOException, InterruptedException, SQLException {
		assertEmpty();
		long delay = 2;
		
		long tsStart = System.currentTimeMillis();
		char[] names = {'A', 'B', 'C', 'D'};
		
		int d = 0;
		for(char name : names) {
			assertTrue( queue.add(new DelayedString(String.valueOf(name), delay+d)) );
			d += 2;
		}

		for(char name : names) {
			assertEquals("First str should be "+name, String.valueOf(name), queue.poll(delay+2, TimeUnit.SECONDS).get());
			assertBetween(System.currentTimeMillis() - tsStart, delay*1000-100, (delay+1)*1000+100);
			tsStart = System.currentTimeMillis();
		}

		assertEmpty();
	}
	
}
