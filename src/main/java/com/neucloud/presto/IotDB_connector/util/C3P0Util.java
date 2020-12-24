package com.neucloud.presto.IotDB_connector.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class C3P0Util {
	private static Logger log = LoggerFactory.getLogger(C3P0Util.class);
	private static ComboPooledDataSource dataSource;

	public static void init(Map<String, String> config) {
		log.info("初始化IotDB连接");
		if (dataSource == null) {
			dataSource = new ComboPooledDataSource();
			String driverClass = (String) config.get("driverClass");
			String url = (String) config.get("url");
			String userName = (String) config.get("userName");
			String passWord = (String) config.get("passWord");
			String connMinSize = (String) config.get("connMinSize");
			String connMaxSize = (String) config.get("connMaxSize");
			String connInitSize = (String) config.get("connInitSize");
			String maxIdleTime = (String) config.get("maxIdleTime");
			try {
				int initConnSize = Integer.parseInt(connInitSize);
				int minConnSize = Integer.parseInt(connMinSize);
				int maxConnSize = Integer.parseInt(connMaxSize);
				int idleTime = Integer.parseInt(maxIdleTime);

				dataSource.setDriverClass(driverClass);
				dataSource.setJdbcUrl(url);
				dataSource.setUser(userName);
				dataSource.setPassword(passWord);
				dataSource.setInitialPoolSize(initConnSize);
				dataSource.setMinPoolSize(minConnSize);
				dataSource.setMaxPoolSize(maxConnSize);
				dataSource.setMaxIdleTime(idleTime);
			} catch (Exception e) {
				e.printStackTrace();
				log.error(e.getMessage());
			}
		}

		log.info("IotDB连接建立完成,{}", config);
	}

	public static Connection getConnection() {
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
		}
		throw new RuntimeException();
	}

	public static void release(Connection conn, Statement stmt, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			rs = null;
		}
		if (stmt != null) {
			try {
				stmt.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			stmt = null;
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			conn = null;
		}
	}
}
