package net.bramp.db_patterns.queues;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bramp.db_patterns.locks.MySQLSleepBasedCondition;
import net.bramp.serializator.Serializator;

/**
 * A queue backed by MySQL
 * <p/>
 * CREATE TABLE queue (
 *     id          INT UNSIGNED NOT NULL AUTO_INCREMENT,
 *     queue_name  VARCHAR(255) NOT NULL,    -- Queue name
 *     inserted    TIMESTAMP NOT NULL,      -- Time the row was inserted
 *     inserted_by VARCHAR(255) NOT NULL,    -- and by who
 *     acquired    TIMESTAMP NULL,          -- Time the row was acquired
 *     acquired_by VARCHAR(255) NULL,        -- and by who
 *     delayed_to  TIMESTAMP NULL,
 *     value       BLOB NOT NULL,           -- The actual data
 *     PRIMARY KEY (id)
 * ) ENGINE=INNODB DEFAULT CHARSET=UTF8;
 * <p/>
 * TODO Create efficient drainTo
 *
 * @param <E>
 * @author bramp
 */
public class MySQLBasedDelayQueue<E extends Delayed> extends MySQLBasedQueue<E> {


	final static protected String delayedAddQuery  = "INSERT INTO queue (queue_name, inserted, inserted_by, delayed_to, value) values (?, now(), ?, DATE_ADD(NOW(), INTERVAL ? SECOND), ?)";
	final static protected String delayedPeekQuery = "SELECT value FROM queue WHERE acquired IS NULL AND (delayed_to<=now() OR delayed_to is null) AND queue_name = ?  ORDER BY id ASC LIMIT 1";
	final static String delayedPollQuery[] = {
		"SET @update_id := -1; ",
		"UPDATE queue SET " +
				"   id = (SELECT @update_id := id), " +
				"   acquired = NOW(), " +
				"   acquired_by = ? " +
				"WHERE "+
				"acquired IS NULL AND " +
				"(delayed_to<=now() OR delayed_to is null) AND "+
				"queue_name = ? " +
				"ORDER BY id ASC " +
				"LIMIT 1; ",
		"SELECT value FROM queue WHERE id = @update_id"
	};

	public MySQLBasedDelayQueue(DataSource ds, String queueName, Class<E> type, String me) {
		super(ds, queueName, type, me);
	}
	
	public MySQLBasedDelayQueue(DataSource ds, String queueName, Serializator<E> serializator, String me) {
		super(ds, queueName, serializator, me);
	}

	public boolean add(E value) {
		try {
			Connection c = ds.getConnection();
			try {
				PreparedStatement s = c.prepareStatement(getAddQuery());
				try {
					

					s.setString(1, queueName);
					s.setObject(2, me); // Inserted by me
					s.setLong(3, value.getDelay(TimeUnit.SECONDS));
					setValueToStatment(s, 4, value);
					s.execute();

					// Wake up one
					condition.signal();

					return true;

				} finally {
					s.close();
				}
			} finally {
				c.close();
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	@Override
	protected String getAddQuery() {
		return delayedAddQuery;
	}

	@Override
	protected String getPeekQuery() {
		return delayedPeekQuery;
	}

	@Override
	protected String[] getPollQuery() {
		return pollQuery;
	}

	@Override
	protected String getCleanupQuery() {
		return cleanupQuery;
	}

	@Override
	protected String getCleanupAllQuery() {
		return cleanupAllQuery;
	}
}
