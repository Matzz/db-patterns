package net.bramp.db_patterns.queues;

import java.sql.PreparedStatement;
import java.sql.SQLException;

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
 *   priority    int(11) NOT NULL DEFAULT '0',                   -- Item priority
 *   value       blob NOT NULL,                                  -- The actual data
 *   PRIMARY KEY (id)
 *   UNIQUE KEY `queue_peek_index` (`acquired`,`queue_name`, `priority`,`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 * <p/>
 * TODO Create efficient drainTo
 *
 * @param <E>
 * @author bramp
 */
public class MySQLBasedQueue<E> extends AbstractMySQLQueue<E> {
	{
		addQuery = "INSERT INTO "+tableNamePlaceholder+" "
			+ "(queue_name, inserted, inserted_by, priority, value) values "
			+ "(?, now(), ?, -?, ?)";
		peekQuery = "SELECT id, status, -priority, value FROM "+tableNamePlaceholder+" WHERE "
				+ "acquired IS NULL "
				+ "AND queue_name = ? "
				+ "ORDER BY priority ASC, id ASC "
				+ "LIMIT 1; ";
		pollQuery = new String[] {
				"SET @update_id := -1; ",
				 "UPDATE "+tableNamePlaceholder+" SET "
				+ " id = (SELECT @update_id := id), "
				+ " acquired = NOW(), "
				+ " acquired_by = ? "
				+ "WHERE "
				+ "acquired IS NULL "
				+ "AND queue_name = ? "
				+ "ORDER BY priority ASC, id ASC "
				+ "LIMIT 1; ",
				"SELECT id, status, -priority, value FROM "+tableNamePlaceholder+" WHERE id = @update_id"
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
	protected void setAddParameters(E value, int priority, PreparedStatement s) throws SQLException {
		s.setString(1, queueName);
		s.setObject(2, me); // Inserted by me
		s.setLong(3, priority); // Inserted by me
		setValueToStatment(s, 4, value);
	}
}
