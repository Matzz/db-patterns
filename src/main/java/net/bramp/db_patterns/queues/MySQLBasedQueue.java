package net.bramp.db_patterns.queues;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import net.bramp.serializator.Serializator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A queue backed by MySQL
 * <p>
 * CREATE TABLE queue (
 *     id          INT UNSIGNED NOT NULL AUTO_INCREMENT,
 *     queue_name  VARCHAR(255) NOT NULL,   -- Queue name
 *     inserted    TIMESTAMP NOT NULL,      -- Time the row was inserted
 *     inserted_by VARCHAR(255) NOT NULL,   -- and by who
 *     acquired    TIMESTAMP NULL,          -- Time the row was acquired
 *     acquired_by VARCHAR(255) NULL,       -- and by who
 *     value       BLOB NOT NULL,           -- The actual data
 *     status      VARCHAR(255) NOT NULL DEFAULT 'NEW'  -- The actual data
 *     PRIMARY KEY (id)
 * ) ENGINE=INNODB DEFAULT CHARSET=UTF8;
 * <p/>
 * TODO Create efficient drainTo
 *
 * @param <E>
 * @author bramp
 */
public class MySQLBasedQueue<E> extends AbstractMySQLQueue<E> {
	{
		addQuery = "INSERT INTO "+tableNamePlaceholder+" "
			+ "(queue_name, inserted, inserted_by, value) values "
			+ "(?, now(), ?, ?)";
		peekQuery = "SELECT id, status, value FROM "+tableNamePlaceholder+" WHERE "
				+ "acquired IS NULL "
				+ "AND queue_name = ? "
				+ "ORDER BY id ASC LIMIT 1";
		pollQuery = new String[] {
				"SET @update_id := -1; ",
				 "UPDATE "+tableNamePlaceholder+" SET "
				+ " id = (SELECT @update_id := id), "
				+ " acquired = NOW(), "
				+ " acquired_by = ? "
				+ "WHERE "
				+ "acquired IS NULL "
				+ "AND queue_name = ? "
				+ "ORDER BY id ASC "
				+ "LIMIT 1; ",
				"SELECT id, status, value FROM "+tableNamePlaceholder+" WHERE id = @update_id"
		};
	}

	/**
	 * Legacy constructor
	 * 
	 * @param ds
	 * @param queueName
	 * @param type
	 * @param me         The name of this node, for storing in the database table
	 * @deprecated
	 */
	public MySQLBasedQueue(DataSource ds, String queueName, Class<E> type, String me) {
		super(ds, "queue", queueName, me);
		this.type = type;
	}

	public MySQLBasedQueue(DataSource ds, String queueTableName, String queueName, Class<E> type, String me) {
		super(ds, queueTableName, queueName, type, me);
	}
 
	public MySQLBasedQueue(DataSource ds, String queueTableName, String queueName, Serializator<E> serializator, String me) {
		super(ds, queueTableName, queueName, serializator, me);
	}

	@Override
	protected void setAddParameters(E value, PreparedStatement s) throws SQLException {
		s.setString(1, queueName);
		s.setObject(2, me); // Inserted by me
		setValueToStatment(s, 3, value);
	}
}
