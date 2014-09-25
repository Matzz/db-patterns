package net.bramp.db_patterns.queues;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

import javax.sql.DataSource;

import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException;

import net.bramp.db_patterns.locks.MySQLSleepBasedCondition;
import net.bramp.db_patterns.queues.interfaces.CleanableQueue;
import net.bramp.db_patterns.queues.interfaces.PriorityQueue;
import net.bramp.db_patterns.queues.interfaces.StatusableQueue;
import net.bramp.serializator.Serializator;

/**
 * TODO Create efficient drainTo
 *
 * @param <E>
 * @author bramp
 */
abstract class AbstractMySQLQueue<E> extends AbstractBlockingQueue<E> implements
		StatusableQueue<E, ValueContainer<E>>, PriorityQueue<E>, CleanableQueue {
	protected String me;
	protected DataSource ds;
	protected String queueName;
	protected String tableName;

	protected Class<E> type = null;
	protected Serializator<E> serializator = null;
	protected Condition condition;
	/**
	 * time in seconds
	 */
	private volatile int takeBlockingTime = 60;

	final static String tableNamePlaceholder = "%TABLE_NAME%";
	protected String addQuery;

	protected String peekQuery;
	protected String[] pollQuery;

	protected String clearQuery = "DELETE FROM " + tableNamePlaceholder
			+ " WHERE queue_name = ? ";

	protected String cleanupQuery = "DELETE FROM " + tableNamePlaceholder
			+ " WHERE acquired IS NOT NULL " + " AND queue_name = ? "
			+ " AND acquired < DATE_SUB(NOW(), INTERVAL ? DAY)";

	protected String cleanupAllQuery = "DELETE FROM " + tableNamePlaceholder
			+ " WHERE acquired IS NOT NULL "
			+ " AND acquired < DATE_SUB(NOW(), INTERVAL ? DAY)";

	protected String updateStatusQuery = "UPDATE " + tableNamePlaceholder
			+ " SET status = ? " + "WHERE id = ? " + "LIMIT 1; ";

	protected String getStatusQuery = "SELECT status FROM queue WHERE id = ?";

	protected String sizeQuery = "SELECT COUNT(*) FROM queue WHERE acquired IS NULL AND queue_name = ?";

	/**
	 * Creates a new MySQL backed queue. Store values using statement setObject.
	 * 
	 * @param ds
	 * @param queueTableName queue table name in database
	 * @param queueTableName
	 * @param queueName queue name in database
	 * @param type value primitive type. Used to store value in database if serializator is not defined.
	 * @param me
	 *            The name of this node, for storing in the database table
	 */
	public AbstractMySQLQueue(DataSource ds, String queueTableName,
			String queueName, Class<E> type, String me) {
		this(ds, queueTableName, queueName, me);
		this.type = type;
	}

	/**
	 * Creates a new MySQL backed queue. Store values using serializator and
	 * setBytes.
	 * 
	 * @param ds datasource
	 * @param queueTableName queue table name in database
	 * @param queueName queue name in database
	 * @param serializator used to store values
	 * @param me The name of this node, for storing in the database table
	 */
	public AbstractMySQLQueue(DataSource ds, String queueTableName,
			String queueName, Serializator<E> serializator, String me) {
		this(ds, queueTableName, queueName, me);
		this.serializator = serializator;
	}

	protected AbstractMySQLQueue(DataSource ds, String tableName,
			String queueName, String me) {
		this.ds = ds;
		this.tableName = escapeTableName(tableName);
		this.queueName = queueName;
		this.condition = new MySQLSleepBasedCondition(ds, "queue-" + queueName);
		this.me = me;
	}

	/**
	 * Gets take operation blocking time. Default 60s.
	 * Unit - seconds.
	 */
	public int getTakeBlockingTime() {
		return takeBlockingTime;
	}

	/**
	 * Sets take operation blocking time. Decrease it to enable faster interruption.
	 * Unit - seconds.
	 */
	public void setTakeBlockingTime(int takeBlockingTime) {
		this.takeBlockingTime = takeBlockingTime;
	}

	@Override
	public boolean add(E value) {
		return add(value, ValueContainer.DEFAULT_PRIORRITY);
	}

	@Override
	public boolean add(E value, int priority) {
		try {
			Connection c = ds.getConnection();
			try {
				PreparedStatement s = c.prepareStatement(getAddQuery());
				try {
					setAddParameters(value, priority, s);
					s.execute();
					wakeupThread();
					return true;

				} finally {
					s.close();
				}
			} finally {
				c.close();
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ValueContainer<E> peekWithMetadata() {
		try {
			Connection c = ds.getConnection();
			try {
				PreparedStatement s = c.prepareStatement(getPeekQuery());
				try {
					s.setString(1, queueName);
					if (s.execute()) {
						ResultSet rs = s.getResultSet();
						if (rs != null && rs.next()) {
							return valueContainerFromResult(rs);
						}
					}
					return null;
				} finally {
					s.close();
				}

			} finally {
				c.close();
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	protected ValueContainer<E> executePollWithMetadata(Connection c,
			String[] pollQuery) throws SQLException {
		PreparedStatement s0 = null;
		PreparedStatement s1 = null;
		PreparedStatement s2 = null;

		try {
			s0 = c.prepareStatement(pollQuery[0]);
			s0.execute();

			s1 = c.prepareStatement(pollQuery[1]);
			s1.setString(1, queueName);
			s1.setString(2, me); // Acquired by me
			s1.execute();

			s2 = c.prepareStatement(pollQuery[2]);
			boolean success = s2.execute();

			c.commit();

			if (success) {
				ResultSet rs = s2.getResultSet();
				if (rs != null && rs.next()) {
					return valueContainerFromResult(rs);
				}
			}
			return null;

		} finally {
			try { if (s0 != null) s0.close(); } catch (Exception e) { }
			try { if (s1 != null) s0.close(); } catch (Exception e) { }
			try { if (s2 != null) s0.close(); } catch (Exception e) { }
		}
	}
	
	AtomicInteger eCnt = new AtomicInteger(0);

	@Override
	public ValueContainer<E> pollWithMetadata() {
		String[] pollQuery = getPollQuery();
		Connection c = null;
		try {
			c = ds.getConnection();
			c.setAutoCommit(false);
			SQLException lastException = null;
			do {
				try {
					return executePollWithMetadata(c, pollQuery);
				}
				catch(SQLException e) {
					c.rollback();
					lastException = e;
				}
			}
			while(lastException instanceof MySQLTransactionRollbackException);
			if(lastException!=null) {
				throw lastException;
			}
			return null;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		finally {
			if (c != null) {
				try {
					c.setAutoCommit(true);
					c.close();
				} catch (Exception ex) {
				}
			}
		}
	}

	@Override
	public ValueContainer<E> takeWithMetadata() throws InterruptedException {
		// We loop around trying to get a item, blocking at most a minute at
		// a time this allows us to be interrupted
		ValueContainer<E> head = null;
		while (head == null) {
			if (Thread.interrupted())
				throw new InterruptedException();

			head = pollWithMetadata(takeBlockingTime, TimeUnit.SECONDS);
		}
		return head;
	}

	@Override
	public ValueContainer<E> pollWithMetadata(long timeout, TimeUnit unit)
			throws InterruptedException {

		final long deadlineMillis = System.currentTimeMillis()
				+ unit.toMillis(timeout);
		final Date deadline = new Date(deadlineMillis);

		ValueContainer<E> head = null;
		boolean stillWaiting = true;

		while (stillWaiting) {
			// Check if we can grab one
			head = pollWithMetadata();
			if (head != null)
				break;

			// Block until we are woken, or deadline
			// Because we don't have a distributed lock around this condition,
			// there is a race condition
			// whereby we might miss a notify(). However, we can somewhat
			// mitigate the problem, by using
			// this in a polling fashion
			stillWaiting = condition.awaitUntil(deadline);
		}

		return head;
	}

	@Override
	public void updateStatus(long id, String newStatus) {
		try {
			Connection c = ds.getConnection();
			String updateStatusQuery = getUpdateStatusQuery();
			try {
				CallableStatement s = c.prepareCall(updateStatusQuery);
				s.setString(1, newStatus);
				s.setLong(2, id);
				s.execute();
			} finally {
				c.close();
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getStatus(long id) {
		try {
			Connection c = ds.getConnection();
			String statusQuery = getStatusQuery();
			try {
				CallableStatement s = c.prepareCall(statusQuery);
				s.setLong(1, id); // Acquired by me

				if (s.execute()) {
					ResultSet rs = s.getResultSet();
					if (rs != null && rs.next()) {
						return rs.getString(1);
					}
				}
				return null;
			} finally {
				c.close();
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public E peek() {
		ValueContainer<E> item = peekWithMetadata();
		return item != null ? item.value : null;
	}

	@Override
	public E poll() {
		ValueContainer<E> item = pollWithMetadata();
		return item != null ? item.value : null;
	}

	@Override
	public E take() throws InterruptedException {
		ValueContainer<E> item = takeWithMetadata();
		return item != null ? item.value : null;
	}

	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		ValueContainer<E> item = pollWithMetadata(timeout, unit);
		return item != null ? item.value : null;
	}

	@Override
	public int size() {
		try {
			Connection c = ds.getConnection();
			try {
				PreparedStatement s = c.prepareStatement(getSizeQuery());
				s.setString(1, queueName);
				s.execute();

				ResultSet rs = s.getResultSet();
				if (rs != null && rs.next())
					return rs.getInt(1);

				throw new RuntimeException("Failed to retreive size");

			} finally {
				c.close();
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void clear() {
		Connection c;
		try {
			c = ds.getConnection();
			try {
				CallableStatement s = c.prepareCall(getClearQuery());
				s.setString(1, queueName);
				s.execute();

			} finally {
				c.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Legacy cleanupAll
	 * 
	 * @deprecated
	 * @throws SQLException
	 */
	public void cleanup() throws SQLException {
		cleanup(10);
	}

	/**
	 * Legacy cleanupAll
	 * 
	 * @deprecated
	 * @throws SQLException
	 */
	public void cleanupAll() throws SQLException {
		cleanupAll(10);
	}

	@Override
	public void cleanup(int days) throws SQLException {
		Connection c = ds.getConnection();
		try {
			CallableStatement s = c.prepareCall(getCleanupAllQuery());
			s.setString(1, queueName);
			s.setInt(2, days);
			s.execute();

		} finally {
			c.close();
		}
	}

	@Override
	public void cleanupAll(int days) throws SQLException {
		Connection c = ds.getConnection();
		try {
			CallableStatement s = c.prepareCall(getCleanupAllQuery());
			s.setInt(1, days);
			s.execute();

		} finally {
			c.close();
		}
	}

	/**
	 * Bind parameters to add query
	 * @param value to add
	 * @param statement
	 * @throws SQLException
	 */
	abstract protected void setAddParameters(E value, int priority,
			PreparedStatement statement) throws SQLException;

	/**
	 * Binds table name to query
	 * @param query
	 * @return query with table name binded
	 */
	protected String setTable(String query) {
		return query.replaceAll(tableNamePlaceholder, tableName);
	}

	/**
	 * Escape table name to prevent SQL injection.
	 * @param tableName
	 * @return escaped against ` char
	 */
	protected String escapeTableName(String tableName) {
		return "`" + tableName.replaceAll("`", "") + "`";
	}

	/**
	 * Wakes up one thread.
	 */
	protected void wakeupThread() {
		condition.signal();
	}

	/**
	 * Get value from result set. Deserialize it if serializator defined otherwise getObject mehtod is used.
	 * @param rs
	 * @param index
	 * @return
	 * @throws SQLException
	 */
	protected E getValueFromResult(ResultSet rs, int index) throws SQLException {
		if (serializator == null) {
			return rs.getObject(index, type);
		} else {
			return serializator.deserialize(rs.getBytes(index));
		}
	}

	/**
	 * Crates ValueContainer form result set.
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	protected ValueContainer<E> valueContainerFromResult(ResultSet rs)
			throws SQLException {
		// id, status, priority, value
		return new ValueContainer<E>(rs.getLong(1), rs.getString(2),
				rs.getLong(3), getValueFromResult(rs, 4));
	}

	/**
	 * Sets value to statement. If defined, serializator is used, otherwise setObject with type.
	 * @param s
	 * @param index
	 * @param obj
	 * @throws SQLException
	 */
	protected void setValueToStatment(PreparedStatement s, int index, E obj)
			throws SQLException {
		if (serializator == null) {
			s.setObject(index, obj);
		} else {
			s.setBytes(index, serializator.serialize(obj));
		}
	}

	/**
	 * Returns sql query for add operation with binded table name
	 * @return sql
	 */
	protected String getAddQuery() {
		return setTable(addQuery);
	}

	/**
	 * Returns sql query for add operation with binded table name
	 * @return sql
	 */
	protected String getPeekQuery() {
		return setTable(peekQuery);
	}

	/**
	 * Returns sql array for poll operation with binded table name
	 * @return sql array
	 */
	protected String[] getPollQuery() {
		String[] queries = new String[pollQuery.length];
		for (int i = 0; i < queries.length; i++) {
			queries[i] = setTable(pollQuery[i]);
		}
		return queries;
	}

	/**
	 * Returns sql for size operation with binded table name
	 * @return sql array
	 */
	protected String getSizeQuery() {
		return setTable(sizeQuery);
	}

	/**
	 * Returns sql for set status operation with binded table name
	 * @return sql array
	 */
	protected String getUpdateStatusQuery() {
		return setTable(updateStatusQuery);
	}

	/**
	 * Returns sql for get status operation with binded table name
	 * @return sql array
	 */
	protected String getStatusQuery() {
		return setTable(getStatusQuery);
	}

	/**
	 * Returns sql for clear operation with binded table name
	 * @return sql array
	 */
	protected String getClearQuery() {
		return setTable(clearQuery);
	}

	/**
	 * Returns sql for cleanup operation with binded table name
	 * @return sql array
	 */
	protected String getCleanupQuery() {
		return setTable(cleanupQuery);
	}

	/**
	 * Returns sql for cleanup all operation with binded table name
	 * @return sql array
	 */
	protected String getCleanupAllQuery() {
		return setTable(cleanupAllQuery);
	}
}
