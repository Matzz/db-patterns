package net.bramp.db_patterns.queues.interfaces;

import java.sql.SQLException;

public interface CleanableQueue {

	/**
	 * Remove all acquired entries from current queue older than specified number of days
	 * @param days
	 * @throws SQLException
	 */
	public void cleanup(int days) throws SQLException;

	/**
	 * Remove all acquired entries from all queues older than specified number of days
	 * @param days
	 * @throws SQLException
	 */
	public void cleanupAll(int days) throws SQLException;
	
}
