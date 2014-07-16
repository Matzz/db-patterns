package net.bramp.db_patterns.queues;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import javax.sql.DataSource;

import net.bramp.db_patterns.locks.MySQLSleepBasedCondition;
import net.bramp.serializator.Serializator;

/**
 * TODO Create efficient drainTo
 *
 * @param <E>
 * @author bramp
 */
abstract class AbstractMySQLQueue<E> extends AbstractBlockingQueue<E> {

	protected String me;
	protected DataSource ds;
	protected String queueName;
	protected String tableName;

	protected Class<E> type = null;
	protected Serializator<E> serializator = null;
	protected Condition condition;

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

	protected String sizeQuery = "SELECT COUNT(*) FROM queue WHERE acquired IS NULL AND queue_name = ?";

	/**
	 * Creates a new MySQL backed queue. Store values using statement setObject.
	 * 
	 * @param ds
	 * @param queueTableName
	 * @param queueName
	 * @param type
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
	 * @param ds
	 * @param queueName
	 * @param serializator
	 * @param me
	 *            The name of this node, for storing in the database table
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

	public boolean add(E value) {
		try {
			Connection c = ds.getConnection();
			try {
				PreparedStatement s = c.prepareStatement(getAddQuery());
				try {
					setAddParameters(value, s);
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

	/**
	 * No blocking
	 */
	public E peek() {
		try {
			Connection c = ds.getConnection();
			try {
				PreparedStatement s = c.prepareStatement(getPeekQuery());
				try {
					s.setString(1, queueName);
					if (s.execute()) {
						ResultSet rs = s.getResultSet();
						if (rs != null && rs.next()) {
							return getValueFromResult(rs, 1);
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

	/**
	 * No blocking
	 */
	public E poll() {
		try {
			Connection c = ds.getConnection();
			String[] pollQuery = getPollQuery();
			try {
				c.setAutoCommit(false);

				CallableStatement s1 = c.prepareCall(pollQuery[0]);
				s1.execute();

				PreparedStatement s2 = c.prepareStatement(pollQuery[1]);
				s2.setString(1, me); // Acquired by me
				s2.setString(2, queueName);
				s2.execute();

				CallableStatement s3 = c.prepareCall(pollQuery[2]);
				s3.execute();

				c.commit();

				if (s3.execute()) {
					ResultSet rs = s3.getResultSet();
					if (rs != null && rs.next()) {
						return getValueFromResult(rs, 1);
					}
				}

				return null;

			} finally {
				c.setAutoCommit(true);
				c.close();
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

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

	/**
	 * Blocks until something is in the queue, up to timeout null if timeout
	 * occurs
	 */
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {

		final long deadlineMillis = System.currentTimeMillis()
				+ unit.toMillis(timeout);
		final Date deadline = new Date(deadlineMillis);

		E head = null;
		boolean stillWaiting = true;

		while (stillWaiting) {
			// Check if we can grab one
			head = poll();
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

	/**
	 * Legacy cleanupAll
	 * 
	 * @deprecated
	 * @throws SQLException
	 */
	public void cleanupAll() throws SQLException {
		cleanupAll(10);
	}

	/**
	 * Cleans up all queues
	 * 
	 * @throws SQLException
	 */
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

	protected void setAddParameters(E value, PreparedStatement s) throws SQLException {
	}

	protected String setTable(String query) {
		return query.replaceAll(tableNamePlaceholder, tableName);
	}

	protected String escapeTableName(String tableName) {
		return "`" + tableName.replaceAll("`", "") + "`";
	}

	protected void wakeupThread() {
		condition.signal();
	}

	protected E getValueFromResult(ResultSet rs, int index) throws SQLException {
		if (serializator == null) {
			return rs.getObject(1, type);
		} else {
			return serializator.deserialize(rs.getBytes(index));
		}
	}

	protected void setValueToStatment(PreparedStatement s, int index, E obj)
			throws SQLException {
		if (serializator == null) {
			s.setObject(index, obj);
		} else {
			s.setBytes(index, serializator.serialize(obj));
		}
	}


	protected String getAddQuery() {
		return setTable(addQuery);
	}

	protected String getPeekQuery() {
		return setTable(peekQuery);
	}

	protected String[] getPollQuery() {
		String[] queries = new String[pollQuery.length];
		for(int i=0; i<queries.length; i++) {
			queries[i] = setTable(pollQuery[i]); 
		}
		return queries;
	}

	protected String getSizeQuery() {
		return setTable(sizeQuery);
	}
	
	protected String getClearQuery() {
		return setTable(clearQuery);
	}

	protected String getCleanupQuery() {
		return setTable(cleanupQuery);
	}

	protected String getCleanupAllQuery() {
		return setTable(cleanupAllQuery);
	}

}
