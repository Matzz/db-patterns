package net.bramp.db_patterns.queues;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import net.bramp.serializator.Serializator;

/**
 * A queue backed by MySQL
 * <p>
 * CREATE TABLE IF NOT EXISTS queue (
 *   id int(10)  unsigned NOT NULL AUTO_INCREMENT,
 *   queue_name  varchar(255) NOT NULL,                          -- Queue name
 *   inserted    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,   -- Time the row was inserted
 *   inserted_by varchar(255) NOT NULL,                          -- and by who
 *   acquired    timestamp NULL DEFAULT NULL,                    -- Time the row was acquired
 *   acquired_by varchar(255) DEFAULT NULL,                      -- and by who
 *   status      varchar(255) NOT NULL DEFAULT 'NEW',            -- Item status
 *   delayed_to  timestamp NULL DEFAULT NULL,                    -- Task delayed to
 *   priority    int(11) NOT NULL DEFAULT '0',                   -- Item priority
 *   value       blob NOT NULL,                                  -- The actual data
 *   PRIMARY KEY (id)
 *   UNIQUE KEY `queue_peek_index` (`acquired`,`queue_name`, `delayed_to`, `priority`,`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 * <p/>
 * TODO Create efficient drainTo
 * 
 * @param <E>
 * @author matzz
 */
public class MySQLBasedDelayQueue<E extends Delayed> extends
		AbstractMySQLQueue<E> {
	 
	protected String closestDelayQuery = "SELECT min(TIME_TO_SEC(TIMEDIFF(delayed_to,NOW()))) FROM " + tableNamePlaceholder
			+ " WHERE acquired IS NULL AND queue_name = ?";

	protected String delayCondition = "AND (delayed_to<=NOW() OR delayed_to is null) ";
	
	{
		addQuery = "INSERT INTO "+tableNamePlaceholder+" "
				+ "(queue_name, inserted, inserted_by, delayed_to, priority, value) values "
				+ "(?, now(), ?, DATE_ADD(NOW(), INTERVAL ? SECOND), -?, ?)";
	
		peekQuery = "SELECT id, status, -priority, value FROM "+tableNamePlaceholder+" WHERE "
				+ "acquired IS NULL "
				+ delayCondition
				+ "AND queue_name = ? "
				+ "ORDER BY priority ASC, id ASC "
				+ "LIMIT 1; ";

		pollQuery = new String[] {
				"SET @update_id := -1; ",
				"SELECT (SELECT @update_id := id), status, -priority, value "
				+ "FROM "+tableNamePlaceholder+" "
				+ "WHERE "
				+ "acquired IS NULL "
				+ delayCondition
				+ "AND queue_name = ? "
				+ "ORDER BY priority ASC, id ASC "
				+ "LIMIT 1 "
				+ "FOR UPDATE",
				"UPDATE "+tableNamePlaceholder+" u "
				+ "SET "
				+ "acquired = NOW(), "
				+ "acquired_by = ? "
				+ "where u.id = @update_id;"
		};
	}



	public MySQLBasedDelayQueue(DataSource ds, String queueTableName,
			String queueName, Class<E> type, String me) {
		super(ds, queueTableName, queueName, type, me);
	}


	public MySQLBasedDelayQueue(DataSource ds, String queueTableName,
			String queueName, Serializator<E> serializator, String me) {
		super(ds, queueTableName, queueName, serializator, me);
	}


	@Override
	protected void setAddParameters(E value, int priority, PreparedStatement s) throws SQLException {
		s.setString(1, queueName);
		s.setObject(2, me); // Inserted by me
		s.setLong(3, value.getDelay(TimeUnit.SECONDS));
		s.setLong(4, priority);
		setValueToStatment(s, 5, value);
	}

	/**
	 * Returns closest task delay.
	 * @return delay in seconds
	 * @throws SQLException
	 */
	protected long getClosestDelay() throws SQLException {
		int minDelay = 0;
		
		String query = setTable(closestDelayQuery);
		Connection c = ds.getConnection();
		
		PreparedStatement s = c.prepareStatement(query);
		s.setString(1, queueName);
		if (s.execute()) {
			ResultSet rs = s.getResultSet();
			if (rs != null && rs.next()) {
				minDelay = rs.getInt(1);
			}
		}
		return minDelay;
	}

	protected ScheduledExecutorService wakeupScheduler = Executors.newScheduledThreadPool(1);
	protected ScheduledFuture<?> wakeupTask = null;
	protected class WakeupTask implements Runnable {
		@Override
		public void run() {
			condition.signal();
			synchronized(wakeupScheduler) {
				wakeupScheduler.schedule(new Runnable() {
					@Override
					public void run() {
						wakeupThread();
					}
				}, 1, TimeUnit.SECONDS);
			}
		}
	};
	
	@Override
	protected void wakeupThread() {
		synchronized (wakeupScheduler) {
				long delaySeconds;
				try {
					delaySeconds = getClosestDelay();
					if(delaySeconds<=0) {
						delaySeconds = 1;
					}
				} catch (SQLException e) {
					delaySeconds = 1;
					e.printStackTrace();
				}
				if(wakeupTask!=null && wakeupTask.getDelay(TimeUnit.SECONDS)>delaySeconds) {
					wakeupTask.cancel(false);
				}
				if(wakeupTask==null || wakeupTask.isDone() || wakeupTask.isCancelled()) {
					wakeupTask = wakeupScheduler.schedule(new WakeupTask(), delaySeconds, TimeUnit.SECONDS);
				}

		}
	}

}
