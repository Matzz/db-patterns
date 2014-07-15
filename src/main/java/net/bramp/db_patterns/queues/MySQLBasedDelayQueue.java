package net.bramp.db_patterns.queues;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import net.bramp.serializator.Serializator;

/**
 * A queue backed by MySQL
 * 
 * CREATE TABLE queue ( id INT UNSIGNED NOT NULL AUTO_INCREMENT, queue_name
 * VARCHAR(255) NOT NULL, -- Queue name inserted TIMESTAMP NOT NULL, -- Time the
 * row was inserted inserted_by VARCHAR(255) NOT NULL, -- and by who acquired
 * TIMESTAMP NULL, -- Time the row was acquired acquired_by VARCHAR(255) NULL,
 * -- and by who delayed_to TIMESTAMP NULL, -- Task delayed to value BLOB NOT
 * NULL, -- The actual data PRIMARY KEY (id) ) ENGINE=INNODB DEFAULT
 * CHARSET=UTF8;
 * 
 * TODO Create efficient drainTo
 * 
 * @param <E>
 * @author matzz
 */
public class MySQLBasedDelayQueue<E extends Delayed> extends
		AbstractMySQLQueue<E> {

	protected String closestDelayQuery = "SELECT min(delayed_to)-NOW() FROM " + tableNamePlaceholder
			+ " WHERE acquired IS NULL AND queue_name = ?";

	protected String delayCondition = "AND (delayed_to<=NOW() OR delayed_to is null) ";
	{
		addQuery = "INSERT INTO "+tableNamePlaceholder+" "
				+ "(queue_name, inserted, inserted_by, delayed_to, value) values "
				+ "(?, now(), ?, DATE_ADD(NOW(), INTERVAL ? SECOND), ?)";
	
		peekQuery = "SELECT value FROM "+tableNamePlaceholder+" WHERE "
				+ "acquired IS NULL "
				+ delayCondition
				+ "AND queue_name = ? "
				+ "ORDER BY id ASC LIMIT 1";

		pollQuery = new String[] {
				"SET @update_id := -1; ",
				 "UPDATE "+tableNamePlaceholder+" SET "
							+ " id = (SELECT @update_id := id), "
							+ " acquired = NOW(), "
							+ " acquired_by = ? "
							+ "WHERE " + "acquired IS NULL " + delayCondition
							+ "AND queue_name = ? " + "ORDER BY id ASC " + "LIMIT 1; ",
				"SELECT value FROM "+tableNamePlaceholder+" WHERE id = @update_id"
		};
	}


	/**
	 * {@inheritDoc}
	 */
	public MySQLBasedDelayQueue(DataSource ds, String queueTableName,
			String queueName, Class<E> type, String me) {
		super(ds, queueTableName, queueName, type, me);
	}

	/**
	 * {@inheritDoc}
	 */
	public MySQLBasedDelayQueue(DataSource ds, String queueTableName,
			String queueName, Serializator<E> serializator, String me) {
		super(ds, queueTableName, queueName, serializator, me);
	}

	@Override
	protected void setAddParameters(E value, PreparedStatement s)
			throws SQLException {
		s.setString(1, queueName);
		s.setObject(2, me); // Inserted by me
		s.setLong(3, value.getDelay(TimeUnit.SECONDS));
		setValueToStatment(s, 4, value);
	}

	@Override
	protected void wakeupThread() {
		condition.signal();
	}

	protected long getClosestDelay() {
		return 1;
	}

}
