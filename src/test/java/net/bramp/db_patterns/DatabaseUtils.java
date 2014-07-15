package net.bramp.db_patterns;

import javax.sql.DataSource;

import com.google.common.base.Throwables;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

public class DatabaseUtils {
	public static DataSource createDataSource() {
		MysqlDataSource ds = new MysqlDataSource();
		Properties prop;
		try {
			prop = getDatabaseProperties();
			ds.setUser(prop.getProperty("user"));
			ds.setPassword(prop.getProperty("password"));
			ds.setServerName(prop.getProperty("serverName"));
			ds.setDatabaseName(prop.getProperty("databaseName"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ds;
	}

    public static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw Throwables.propagate(e);
        }
    }
    
    protected static Properties getDatabaseProperties() throws IOException {
    	Properties prop = new Properties();
    	InputStream is = null;
    	try {
    		is = DatabaseUtils.class.getClassLoader().getResourceAsStream("database.properties");
    	    prop.load(is);

    	} 
    	finally {
    	    if(is!=null) {
    	    	is.close();
    	    }
    	}
    	return prop;
    }
}
